#!/bin/bash

servers="testing-kafka-dev01:9092,testing-kafka-dev02:9092,testing-kafka-dev03:9092"

echo "success,size,topic,acks,compression,inflight,batch,linger,time,rate,latency"

while IFS= read -r line; do
    if [[ $line == 0,* ]]; then
        IFS=',' read -ra values <<< "${line}"
        size=${values[1]}
        records=$((268435456 / $size))
        topic=${values[2]}
        acks=${values[3]}
        compression=${values[4]}
        inflight=${values[5]}
        batch=${values[6]}
        linger=${values[7]}
    fi

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
    metrics="$(kafka-producer-perf-test --topic ${topic} --num-records ${records} --record-size ${size} --throughput -1 --producer-props max.request.size=10485760 bootstrap.servers=${servers} client.id=test acks=${acks} compression.type=${compression} max.in.flight.requests.per.connection=${inflight} batch.size=${batch} linger.ms=${linger})"
    time=$(($(date +%s%3N) - ${start}))

    echo -n "1,${size},$topic,$acks,$compression,$inflight,$batch,$linger,$time"

    # get metrics result with regex
    while IFS= read -r metric; do 
        regex="${records} records sent, .+ records/sec \((.+) MB/sec\), (.+) ms avg latency.*"
        if [[ "${metric}" =~ $regex ]]; then
            echo ",${BASH_REMATCH[1]},${BASH_REMATCH[2]}"
        fi
    done <<< "$metrics"
done < "$1"


