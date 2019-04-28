from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

DATA_LOCATION = "data"
SORTED_FILE = "sorted_data.txt"
LOCATION_FILE = "space_location.txt"

# containers
date_times = []
location_ids = []
sensor_types = []
measurements = []
voltages = []
municipalities = []

# picked random time in scope
current_date = datetime.strptime("2017-03-15 00:00:00.001", "%Y-%m-%d %H:%M:%S.%f")

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
    #sensor_types.append(int(sensor_type))
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

print("LOG: old data identified")

# sent all previous information to Kafka
for i in range(curr_start):
    loc_id = location_ids[i]
    loc_municipality = municipalities[loc_id - 1]
    print("{} -> {}".format(loc_id, loc_municipality))
