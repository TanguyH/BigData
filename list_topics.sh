#!/bin/bash
SERVER_PORT=2181
./kafka_2.11-1.0.0/bin/kafka-topics.sh --list --zookeeper localhost:$SERVER_PORT
