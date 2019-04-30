import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pathlib import Path
import glob
import os

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


STREAM_IN = "stream-IN/"

struct_files = STREAM_IN + "sensor_type-"+sensor_type		#structure of the batch files for this sensor_type

# We first delete all files from the STREAM_IN folder
# before starting spark streaming.
# This way, all files are new

files = glob.glob(struct_files+"*")
for file in files:

	print("Deleting existing file in %s ..." % STREAM_IN)
	os.remove(file)
	print("... done")

batch_id = 0		#Increment to create different files according to each batch
while True:
	file_in = struct_files + "_"+str(batch_id)+ ".tmp"

	try:
		f = open(file_in, "w")
		consumer = KafkaConsumer(bootstrap_servers = ['localhost:9092'])
		consumer.subscribe(sensor_type)
		for row in consumer:
			f.write(transformRow(row))
			f.write("\n")

	except Exception as err:
		print("Unexpected error: %s" % (err))
	finally:
		consumer.close()
		f.close()
		batch_id += 1		