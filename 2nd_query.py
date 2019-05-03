from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import os
from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from operator import add
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pandas as pd
import pymongo



def parseRow(row):
    '''parses a single row into a dictionary'''
    try:
        v = row.split(" ")
        date_time = datetime.strptime(v[1] + " "+ v[2], "%Y-%m-%d %H:%M:%S.%f")
        day = date_time.date()  #Retrieve day
        time = date_time.time() #Retrieve time (rest of info)
        if time.minute % 15 == 0 and time.second == 0 and time.microsecond == 0:
            slot = 4*time.hour + time.minute//15 - 1
        else:
            slot = 4*time.hour + time.minute//15
        return [{"sensor_type": int(v[0]),
                "time": date_time,
                "slot": slot,
                "p-i": v[3],
                "measurement":float(v[4]),
                "voltage": float(v[5])}]

    except Exception as err:
        print("Unexpected error: %s" % (err))

def storeRdd(rdd, spark_session, schema, collection):
    info_batch = spark_session.createDataFrame(rdd, schema)
    df = info_batch.toPandas().to_dict('records')
    if df != []:
        collection.insert_many(info_batch.toPandas().to_dict('records'))
    print("storedRdd")

# declare database
DB_NAME = "big_data"

# define client & used DB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
used_database = mongo_client[DB_NAME]
db_list = mongo_client.list_database_names()
timeslots = used_database["timeslots"]

# verify existence of DB
if DB_NAME in db_list:
    print("Database exists")
else:
    print("Database does not exist, BUT if it is empty it is normal !")

STREAM_IN = "stream-IN"
print("Deleting existing files in %s ..." % STREAM_IN)
p = Path('.') / STREAM_IN
for f in p.glob("*.tmp"):
    os.remove(f)
print("... done")
sc = SparkContext("local[*]", "test")
sc.setLogLevel("WARN")   #Make sure warnings and errors observed by spark are printed.
ssc = StreamingContext(sc, 30)  #generate a mini-batch every 30 seconds

filestream = ssc.textFileStream(STREAM_IN) #monitor new files in folder stream-IN



# add column to DB
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()


schema = StructType([
    StructField("sensor_type",IntegerType(), True),
    StructField("time",TimestampType(), True),
    StructField("slot",IntegerType(), True),
    StructField("p-i",StringType(), True),
    StructField("measurement",FloatType(), True),
    StructField("voltage",FloatType(), True)
    ])


rows = filestream.flatMap(parseRow).filter(lambda r: int(r["p-i"].split("-")[1]) == 0)

rows.foreachRDD(lambda rdd: storeRdd(rdd, my_spark, schema, timeslots))

ssc.start()
ssc.awaitTermination()