import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pathlib import Path



def transformRow(row):
    values = row.value.decode().split(" ")
    try:
    	return row.topic + " " + values[0] + " " + values[1] + " " + values[2] + " " + values[3] + " " + values[4]
    except Exception as err:
        print("Unexpected error: %s" % (err))


if len(sys.argv) != 2:
	print("You must call this python file by giving a sensor type in argument")
	exit()

sensor_type = sys.argv[1]
if sensor_type not in ["0", "1", "2", "3"]:
	print("Sensor type value must be in [0,3]")
	exit()


STREAM_IN = 'stream-IN/sensor_type-'+sensor_type+".tmp"





try:
	f = open(STREAM_IN, "w")
	consumer = KafkaConsumer(bootstrap_servers = ['localhost:9092'])
	consumer.subscribe(sensor_type)
	for row in consumer:

		row = next(consumer)
		f.write(transformRow(row))
		f.write("\n")

except Exception as err:
	print("Unexpected error: %s" % (err))
finally:
	consumer.close()
	f.close()		