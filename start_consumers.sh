sensorTypes='0 1 2 3'
SERVER_PORT=2181

start_command=""

for sensor_type in $sensorTypes; do
    start_command="$start_command python3 kafka_consumer.py $sensor_type &"
done

eval $start_command
