sh start_zookeeper.sh & sh start_kafka.sh & mongod --dbpath ../data/db & python3 municipality_dispatcher.py
