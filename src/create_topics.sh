#!/bin/bash
sensorTypes='0 1 2 3'
SERVER_PORT=2181

for sensor_type in $sensorTypes; do
    ../kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:$SERVER_PORT --replication-factor 1 --partitions 1 --topic $sensor_type
done
