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
        return [{"sensor_type": int(v[0]),
                 "time": datetime.strptime(v[1] + " "+ v[2], "%Y-%m-%d %H:%M:%S.%f"),
                 "p-i": v[3],
                 "measurement": round(float(v[4]),1),       #rounded precision to 1 digit (follow the requirements)
                 "voltage": float(v[5]),
                "municipality": v[6]}]
    except Exception as err:
        print("Unexpected error: %s" % (err))


def storeRdd(rdd, spark_session, schema, collection):
    info_batch = spark_session.createDataFrame(rdd, schema)
    df = info_batch.toPandas().to_dict('records')
    if df != []:
        print(df)
        collection.insert_many(info_batch.toPandas().to_dict('records'))
        print("storedRdd")
    print("End call storedRdd")

# declare database
DB_NAME = "big_data"

# define client & used DB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
used_database = mongo_client[DB_NAME]
db_list = mongo_client.list_database_names()
window_collection = used_database["window"]  #Collection top_frequency



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
ssc = StreamingContext(sc, 5)  #generate a mini-batch every 5 seconds

filestream = ssc.textFileStream(STREAM_IN) #monitor new files in folder stream-IN

# add column to DB
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()


schema = StructType([
    StructField("measurement",FloatType(), True),   #Measurement that appeared with a high (arbitrary) frequency during the last 1h window
    StructField("frequency",FloatType(), True),     #Associated frequency
    StructField("time", TimestampType(), True)      #datetime of the last element of the current batch (take care of the window aspect in db)
    ])


window_time = 60*60 #one hour
window_slide = 10   #15sec
freq_tresh = 1./20
rows = filestream.flatMap(parseRow).filter(lambda r: int(r["p-i"].split("-")[1]) == 0)      #Get only the temperature sensors

last_time = rows.map(lambda r:(r["time"])).reduce(lambda r1, r2: max(r1, r2))       #Last time object of the batch


num_per_value = rows.map(lambda r: (r["measurement"])).countByValue().window(window_time,window_slide)  #Last hour, window slide = 15sec

total_count = rows.count().window(window_time,window_slide).reduce(add)        #Get the total number of record within the window -> to compute relative frequencies

freq_per_value = num_per_value.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),total_count)\
                                .map(lambda r: (r[0][0], r[0][1]/r[1]))

top_freq = freq_per_value.transform(lambda rdd: rdd.sortBy(lambda r: r[1], False)).filter(lambda r: r[1] > freq_tresh)

top_freq_date = top_freq.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),last_time)\
                                .map(lambda r: (r[0][0], r[0][1], r[1]))

top_freq_date.pprint()
top_freq_date.foreachRDD(lambda rdd: storeRdd(rdd, my_spark, schema, window_collection))

sc.setCheckpointDir("checkpoint")
ssc.start()
ssc.awaitTermination()