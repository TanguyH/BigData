import pymongo

# declare database
DB_NAME = "big_data"

# define client & used DB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
used_database = mongo_client[DB_NAME]
db_list = mongo_client.list_database_names()

# verify existence of DB
if DB_NAME in db_list:
    print("Database exists")
else:
    print("Database does not exist, BUT if it is empty it is normal !")

statistics = used_database["statistics"]
municipalities = used_database["municipality_spaces"]
sensors = used_database["space_sensors"]
timeslots = used_database["timeslots"]
window = used_database["window"]

statistics.drop()
municipalities.drop()
sensors.drop()
timeslots.drop()
window.drop()
