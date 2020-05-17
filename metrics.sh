#!/bin/bash

servers="testing-kafka-dev01:9092,testing-kafka-dev02:9092,testing-kafka-dev03:9092"

# rec size 128 256 1k 8k 32k 64k 128k 512k 1m 2m 4m 
# batch    0 512 1k 8k 16k 32k 64k 128k 512k 1m 2m 4m 8m
# linger   0 20 50 100 150 200 300 500 700 1000

declare -A pairs=( [2097152]=128 [1048576]=256 [262144]=1024 [32768]=8192 [8192]=32768 [4096]=65536 [2048]=131072 [512]=524288 [256]=1048576 [128]=2097152 [64]=4194304 )
#declare -A pairs=( [100]=1024 )

echo "success,size,topic,acks,compression,inflight,batch,linger,time,rate,latency"

for records in "${!pairs[@]}"; do
    for topic in "test-snappy"; do
        for acks in -1 0 1; do
            for compression in "snappy"; do
                for inflight in 1 5; do
                    for batch in 0 512 1024 8192 16384 32768 65536 131072 524288 1048576 2097152 4194304 8388608; do
                        for linger in 0 20 50 100 150 200 300 500 700 1000; do
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
                            metrics="$(timeout 15s kafka-producer-perf-test --topic ${topic} --num-records ${records} --record-size ${pairs[$records]} --throughput -1 --producer-props max.request.size=10485760 bootstrap.servers=${servers} client.id=test acks=${acks} compression.type=${compression} max.in.flight.requests.per.connection=${inflight} batch.size=${batch} linger.ms=${linger})"
                            if [[ $? == 124 ]]; then
                                time=$(($(date +%s%3N) - ${start}))
                                echo "0,${pairs[$records]},$topic,$acks,$compression,$inflight,$batch,$linger,$time,-1,-1"
                                echo
                                continue
                            fi

                            time=$(($(date +%s%3N) - ${start}))
                            
                            echo -n "1,${pairs[$records]},$topic,$acks,$compression,$inflight,$batch,$linger,$time"

                            # get metrics result with regex
                            while IFS= read -r metric; do 
                                regex="${records} records sent, .+ records/sec \((.+) MB/sec\), (.+) ms avg latency.*"
                                if [[ "${metric}" =~ $regex ]]; then
                                    echo ",${BASH_REMATCH[1]},${BASH_REMATCH[2]}"
                                fi
                            done <<< "$metrics"
                        done
                    done
                done
            done
        done
    done
done