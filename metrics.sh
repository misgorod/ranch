#!/bin/bash

servers="testing-kafka-dev01:9092,testing-kafka-dev02:9092,testing-kafka-dev03:9092"

declare -A pairs=( [512000]=1024 [4096000]=128 [500]=1048576 [1000]=524288 [4000]=131072 )
#declare -A pairs=( [100]=1024 )

for records in "${!pairs[@]}"; do
    for topic in "test-snappy" "test-none"; do
        for acks in -1 0 1; do
            for compression in "snappy" "none" "gzip"; do
                for inflight in 1 2 5 7 10; do
                    for batch in 0 16384 131072 524288 1048576 9437184; do
                        for linger in 0 20 50 150 500; do
                            if [[ "${records}" == 512000 ]] && [[ "${topic}" == "test-snappy" ]]; then
                                continue
                            fi

                            if [[ "${records}" == 512000 ]] && [[ "${acks}" != 1 ]]; then
                                continue
                            fi

                            ncout="$(nc -zv testing-kafka-dev01 9092 2>&1 && nc -zv testing-kafka-dev02 9092 2>&1 && nc -zv testing-kafka-dev03 9092 2>&1)"
                            while [[ $? != 0 ]]; do
                                echo "$records,${pairs[$records]},$acks,\"$compression\",$inflight,$batch,$linger,BROKER BROKEN: ${ncout}"
                                sleep 60
                                ncout="$(nc -zv testing-kafka-dev01 9092 2>&1 && nc -zv testing-kafka-dev02 9092 2>&1 && nc -zv testing-kafka-dev03 9092 2>&1)"
                            done

                            (kafka-leader-election --bootstrap-server testing-kafka-dev01:9092,testing-kafka-dev02:9092,testing-kafka-dev03:9092 --election-type preferred --all-topic-partitions)
                            
                            start=$(date +%s%3N)

                            metrics="$(timeout 15s kafka-producer-perf-test --topic ${topic} --num-records ${records} --record-size ${pairs[$records]} --throughput -1 --print-metrics --producer-props bootstrap.servers=${servers} client.id=test acks=${acks} compression.type=${compression} max.in.flight.requests.per.connection=${inflight} batch.size=${batch} linger.ms=${linger})"
                            if [[ $? == 124 ]]; then
                                echo "$records,${pairs[$records]},$acks,\"$compression\",$inflight,$batch,$linger,TIMEOUT"
                                echo
                                continue
                            fi

                            time=$(($(date +%s%3N) - ${start}))
                            
                            echo "$records,${pairs[$records]},$acks,\"$compression\",$inflight,$batch,$linger,$time"

                            while IFS= read -r metric; do 
                                regex="${records} records sent.*"
                                if [[ "${metric}" =~ $regex ]]; then
                                    echo "$metric"
                                fi
                                regex='producer-metrics:(.*):\{.*\}\s+: (.*)'
                                if [[ "${metric}" =~ $regex ]]; then
                                    echo "$metric"
                                fi
                                regex='producer-node-metrics:(.*):\{.*node-id=node-(.*)\}\s+: (.*)'
                                if [[ "${metric}" =~ $regex ]]; then
                                    echo "$metric"
                                fi
                                regex='producer-topic-metrics:(.*):\{.*\}\s+: (.*)'
                                if [[ "${metric}" =~ $regex ]]; then
                                    echo "$metric"
                                fi
                            done <<< "$metrics"

                            sleep 1
                        done
                    done
                done
            done
        done
    done
done