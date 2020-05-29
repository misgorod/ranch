#!/bin/bash
topic="test-snappy"
echo "success,type,size,time,rate,latency"

for type in "ranch" "default"; do
    for size in configs/$type/*; do
        size=$(basename $size)
        records=$((536870912 / size))

        # check if all brokers are alive
        ncout="$(nc -zv testing-kafka-dev01 9092 2>&1 && nc -zv testing-kafka-dev02 9092 2>&1 && nc -zv testing-kafka-dev03 9092 2>&1)"
        while [[ $? != 0 ]]; do
            >&2 echo "$(date) BROKER BROKEN"
            sleep 60
            ncout="$(nc -zv testing-kafka-dev01 9092 2>&1 && nc -zv testing-kafka-dev02 9092 2>&1 && nc -zv testing-kafka-dev03 9092 2>&1)"
        done

        # run leader election to evenly distribute partitions between brokers
        (kafka-leader-election --bootstrap-server testing-kafka-dev01:9092,testing-kafka-dev02:9092,testing-kafka-dev03:9092 --election-type preferred --all-topic-partitions)

        start=$(date +%s%3N)

        # start collecting metrics printing result if there was timeout
        metrics="$(timeout 25s kafka-producer-perf-test --topic ${topic} --num-records ${records} --record-size ${size} --throughput -1 --producer.config configs/$type/$size )"
        if [[ $? == 124 ]]; then
            time=$(($(date +%s%3N) - ${start}))
            echo "0,$type,$size,$time,-1,-1"
            echo
            continue
        fi

        time=$(($(date +%s%3N) - ${start}))

        echo -n "1,$type,$size,$time"

        # get metrics result with regex
        while IFS= read -r metric; do 
            regex="${records} records sent, .+ records/sec \((.+) MB/sec\), (.+) ms avg latency.*"
            if [[ "${metric}" =~ $regex ]]; then
                echo ",${BASH_REMATCH[1]},${BASH_REMATCH[2]}"
            fi
        done <<< "$metrics"
    done
done
