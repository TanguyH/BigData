from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import *
import time

DATA_LOCATION = "../data"
SORTED_FILE = "sorted_data.txt"
SLEEP = 0.5

# containers
date_times = []
location_ids = []
sensor_types = []
readings = []


# define data destination
data_file = "{}/{}".format(DATA_LOCATION, SORTED_FILE)
sorted_info = open(data_file, "r")


#locations.close()
info_lines = sorted_info.readlines()



second = timedelta(seconds=30)
lines_i = 0
sensor_reading = info_lines[lines_i].strip()

record_date, record_time, sensor_id, measurement, voltage = sensor_reading.split(" ")
location_id, sensor_type = sensor_id.split("-")

# convert to datetime
current_date = datetime.strptime("{} {}".format(record_date, record_time), "%Y-%m-%d %H:%M:%S.%f")
try:
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])
    all_processed = False

    while not all_processed:

        next_date = current_date + second
        reached_current_date = False
        while not reached_current_date and not all_processed:
            sensor_reading = info_lines[lines_i].strip()

            record_date, record_time, sensor_id, measurement, voltage = sensor_reading.split(" ")
            location_id, sensor_type = sensor_id.split("-")

            # convert to datetime
            explored_date = datetime.strptime("{} {}".format(record_date, record_time), "%Y-%m-%d %H:%M:%S.%f")
            if explored_date > next_date:
                reached_current_date = True
                current_date = explored_date
                print("LOG: found new interval")
            else:

                topic = "{}".format(sensor_type)
                producer.send(topic, "{}".format(sensor_reading).encode())
                lines_i += 1

                # all data processed
                if lines_i == len(info_lines):
                    all_processed = True
                    print("LOG: all data processed")
        time.sleep(SLEEP)

except KeyboardInterrupt:
     print("Terminating ...")
finally:
    producer.close()
