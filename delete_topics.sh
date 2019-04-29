#!/bin/bash
municipalitiesList='1000 1030 1040 1050 1060 1070 1080 1081 1082 1083 1090 1140 1150 1160 1170 1180 1190 1200 1210'
sensorTypes='0 1 2 3'
SERVER_PORT=2181

for municipality in $municipalitiesList; do
    for sensor_type in $sensorTypes; do
        ./kafka_2.11-1.0.0/bin/kafka-topics.sh --zookeeper localhost:$SERVER_PORT --delete --topic $municipality-$sensor_type
    done
done
