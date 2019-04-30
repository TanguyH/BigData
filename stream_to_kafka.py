from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import *
import time

DATA_LOCATION = "data"
SORTED_FILE = "sorted_data.txt"
LOCATION_FILE = "space_location.txt"
SLEEP = 0.5

# containers
date_times = []
location_ids = []
sensor_types = []
readings = []
municipalities = []

# picked random time in scope

current_date = datetime.strptime("2017-02-28 00:01:19.196", "%Y-%m-%d %H:%M:%S.%f")
#current_date = datetime.strptime("2017-03-05 00:59:16.803", "%Y-%m-%d %H:%M:%S.%f")

# define data destination
data_file = "{}/{}".format(DATA_LOCATION, SORTED_FILE)
sorted_info = open(data_file, "r")

location_file = "{}/{}".format(DATA_LOCATION, LOCATION_FILE)
locations = open(location_file, "r")

# unpack all locations
for location in locations.readlines():
    location = location.strip()
    location_id, location_municipality, location_privacy = location.split(" ")
    municipalities.append(location_municipality)

locations.close()

# unpack all data
for sensor_reading in sorted_info.readlines():
    sensor_reading = sensor_reading.strip()

    record_date, record_time, sensor_id, measurement, voltage = sensor_reading.split(" ")
    location_id, sensor_type = sensor_id.split("-")

    # convert to datetime
    date_time = datetime.strptime("{} {}".format(record_date, record_time), "%Y-%m-%d %H:%M:%S.%f")
    date_times.append(date_time)

    # store information
    location_ids.append(int(location_id))
    sensor_types.append(int(sensor_type))
    readings.append(sensor_reading)

    #if sensor_type != "3":
    #    measurements.append(float(measurement))
    #else:
    #    measurements.append(bool(measurement))
    #voltages.append(float(voltage))

sorted_info.close()
print("LOG: data unpacked")

curr_start = 0
reached_current_date = False
while not reached_current_date:
    explored_date = date_times[curr_start]
    if explored_date > current_date:
        reached_current_date = True
    else:
        curr_start += 1

print("LOG: past data identified")

second = timedelta(seconds=1)

try:
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])
    all_processed = False

    while not all_processed:
        # sent all previous information to Kafka
        for i in range(curr_start):
            loc_id = location_ids[i]
            loc_municipality = municipalities[loc_id - 1]
            loc_sensor_type = sensor_types[i]

            topic = "{}".format(loc_sensor_type)
            producer.send(topic, "{} {}".format(readings[i], loc_municipality).encode())
            #producer.send(topic, [readings[i].encode(), loc_municipality])

        print("LOG: sent 1s interval data")

        # remove sent elements from arrays
        if not all_processed:
            date_times = date_times[curr_start:]
            location_ids = location_ids[curr_start:]
            sensor_types = sensor_types[curr_start:]
            readings = readings[curr_start:]
            print("LOG: next second data identified")

        # prepare new batch to send every second
        current_date = current_date + second

        # find elements of the next second
        curr_start = 0
        reached_current_date = False
        while not reached_current_date and not all_processed:
            explored_date = date_times[curr_start]

            if explored_date > current_date:
                reached_current_date = True
                print("LOG: found new interval")
            else:
                curr_start += 1

                # all data processed
                if curr_start == len(date_times):
                    all_processed = True
                    print("LOG: all data processed")
        time.sleep(SLEEP)

except KeyboardInterrupt:
     print("Terminating ...")
finally:
    producer.close()
