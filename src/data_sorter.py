from datetime import datetime
import numpy as np

DATA_LOCATION = "../data"
ORIGINAL_FILE = "data.conv.txt"
SORTED_FILE = "sorted_data.txt"

# define data source & destination
source_file = "{}/{}".format(DATA_LOCATION, ORIGINAL_FILE)
sensor_info = open(source_file, "r")

# define data destination
destination_file = "{}/{}".format(DATA_LOCATION, SORTED_FILE)
sorted_info = open(destination_file, "w")

# containers
date_times = []
processed_data = []

print("LOG: starting extraction from file")

# read and store data from file
for sensor_reading in sensor_info.readlines():
    sensor_reading = sensor_reading.strip()

    # split information
    record_date, record_time, sensor_id, measurement, voltage = sensor_reading.split(" ")
    #location_id, sensor_type = sensor_id.split("-")

    # avoid faulty records
    if measurement != "":
        # convert to datetime
        date_time = datetime.strptime("{} {}".format(record_date, record_time), "%Y-%m-%d %H:%M:%S.%f")
        date_times.append(date_time)

        # store
        processed_data.append(sensor_reading)

# convert to numpy arrays
print("LOG: end of storage procedure")
sorted_indexes = np.argsort(date_times)
print("LOG: end of date sorting")
processed_data = np.asarray(processed_data)
processed_data = processed_data[sorted_indexes]
print("LOG: processed data sorted and")

for processed_reading in processed_data:
    sorted_info.write("{}\n".format(processed_reading))

print("LOG: processed data stored in new file")

sensor_info.close()
sorted_info.close()
