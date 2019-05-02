import random
from os import listdir
from os.path import isfile, join

NUMBER_OF_SPACES = 10000
NUMBER_OF_SENSORS = 100000
MUNICIPALITIES = [1000, 1030, 1040, 1050, 1060, 1070, 1080, 1081, 1082, 1083, 1090, 1140, 1150, 1160, 1170, 1180, 1190, 1200, 1210]

PRIVACY = ["private", "public"]
DATA_DIRECTORY = "data"
LOCATION_SOURCE = "{}/locations/".format(DATA_DIRECTORY)
SPACE_FILE = "{}/space_location.txt".format(DATA_DIRECTORY)

space_files = [f for f in listdir(LOCATION_SOURCE) if isfile(join(LOCATION_SOURCE, f))]
space_location = open(SPACE_FILE, "w")

space_id = 1
to_dispatch = [i+1 for i in range(NUMBER_OF_SENSORS)]

for file in space_files:
    # attribute municipality & privacy
    attributed_municipality = random.choice(MUNICIPALITIES)
    attributed_privacy = random.choice(PRIVACY)

    # open space file
    space_file = open("{}/{}".format(LOCATION_SOURCE, file), "r")

    # extract sensor information
    for line in space_file.readlines():
        sensor_number, x_coord, y_coord = line.strip().split(" ")
        to_dispatch.remove(int(sensor_number))
        space_location.write("{} {} {} {} {} {}\n".format(space_id, sensor_number, x_coord, y_coord, attributed_municipality, attributed_privacy))
    space_file.close()
space_location.close()

# commented : part that allows automatic attribution of sensors to newly created spaces
"""
# attribute sensors to remaining spaces
remaining_spaces = [0 for i in range(NUMBER_OF_SPACES - space_id)]
for i in range(len(to_dispatch)):
    index_getting_sensor = random.randint(0, len(remaining_spaces) - 1)
    remaining_spaces[index_getting_sensor] += 1

# generate remaining spaces
i = 0
for sensor_count in remaining_spaces:
    attributed_municipality = random.choice(MUNICIPALITIES)
    attributed_privacy = random.choice(PRIVACY)

    # open space file
    file = "location_{}.txt".format(space_id)
    space_file = open("{}/{}".format(LOCATION_SOURCE, file), "w")

    # generate size of space
    x_range = random.randint(5, 100)
    y_range = random.randint(5, 100)

    for sensor_nb in range(sensor_count):
        sensor_number = to_dispatch[i]
        i += 1

        # particular sensor coordinates
        x_coord = random.randint(0, x_range)
        half = random.randint(0, 1)
        if half:
            x_coord += .5
        y_coord = random.randint(0, y_range)

        space_file.write("{} {} {} {} {} {}\n".format(space_id, sensor_number, x_coord, y_coord, attributed_municipality, attributed_privacy))

    space_file.close()
    space_id += 1
"""
