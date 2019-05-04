import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pathlib import Path
import glob
import os
import time
import shutil

BATCH_TIME = 30

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

TMP_DATA = "../data/tmp_data/"
STREAM_IN = "../stream-IN/"
struct_tmp_file = TMP_DATA + "sensor_type-"+sensor_type     #structure of the batch files for this sensor_type
# We first delete all files from the TMP_DATA folder


files = glob.glob(struct_tmp_file+"*")
for file in files:

    print("Deleting existing file in %s ..." % TMP_DATA)
    os.remove(file)
    print("... done")
# initialize consumer
consumer = KafkaConsumer(sensor_type,
                        bootstrap_servers = ['localhost:9092'],
                        auto_offset_reset="earliest",
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000)

# initialize first batch ID & consume flag
consuming = True
batch_id = 0
while consuming:
    file_in = "{}_{}.tmp".format(struct_tmp_file, batch_id)
    f = open(file_in, "w")
    checkpoint = time.time()
    try:
        for row in consumer:

            # handle files
            if time.time() > checkpoint + BATCH_TIME:
                # handle closing
                f.close()
                batch_id += 1

                file_in = "{}_{}.tmp".format(struct_tmp_file, batch_id)
                f = open(file_in, "w")
                checkpoint = time.time()


            f.write("{}\n".format(transformRow(row)))
            shutil.copy(file_in, STREAM_IN)
    except KeyboardInterrupt as err:
        print("LOG: Terminating consumer execution for {}".format(sensor_type))
        consuming = False
    finally:
        print("LOG: closing consumer {} down".format(sensor_type))
        consumer.close()
        f.close()
