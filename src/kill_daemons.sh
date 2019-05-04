sh start_zookeeper.sh & sh start_kafka.sh & mongod --dbpath ../data/db & python3 municipality_dispatcher.py
pid=$(pgrep -f start_zookeeper.sh)
kill $pid

pid=$(pgrep -f start_kafka.sh)
kill $pid

pid=$(pgrep -f mongod)
kill $pid

pid=$(pgrep -f backend_queries.py)
kill $pid

pid=$(pgrep -f kafka_consumer.py)

for cur_pid in $pid; do
	kill $cur_pid
done
