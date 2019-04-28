import random

NUMBER_OF_SPACES = 10000
MUNICIPALITIES = [1000, 1030, 1040, 1050, 1060, 1070, 1080, 1081, 1082, 1083, 1090, 1140, 1150, 1160, 1170, 1180, 1190, 1200, 1210]
PRIVACY = ["private", "public"]
SPACE_FILE = "data/space_location.txt"

space_file = open(SPACE_FILE, "w")

for space_id in range(NUMBER_OF_SPACES):
    attributed_municipality = random.choice(MUNICIPALITIES)
    attributed_privacy = random.choice(PRIVACY)
    space_file.write("{} {} {}\n".format(space_id+1, attributed_municipality, attributed_privacy))

space_file.close()
