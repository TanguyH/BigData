from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import os
from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from operator import add
from kafka import KafkaConsumer
from kafka.errors import KafkaError
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
                 "voltage": float(v[5])}]
    except Exception as err:
        print("Unexpected error: %s" % (err))


def parseRowTimeSlots(row):
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
	if info_batch != None:
		try:
			df_tmp = info_batch.toPandas()
			if not df_tmp.empty:
				df = df_tmp.to_dict('records')
				if df != []:
					collection.insert_many(df)
					print("storedRdd")
		except Exception as err:
			print("Unexpected error: %s" % (err))


	print("End call storedRdd")


# declare database
DB_NAME = "big_data"

# define client & used DB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
used_database = mongo_client[DB_NAME]
db_list = mongo_client.list_database_names()
window_collection = used_database["window"]  #Collection top_frequency
timeslots = used_database["timeslots"]
statistics = used_database["statistics"]


# verify existence of DB
if DB_NAME in db_list:
    print("Database exists")
else:
    print("Database does not exist, BUT if it is empty it is normal !")

STREAM_IN = "../stream-IN"
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


schema_window = StructType([
    StructField("measurement",DoubleType(), True),   #Measurement that appeared with a high (arbitrary) frequency during the last 1h window
    StructField("frequency",DoubleType(), True),     #Associated frequency
    StructField("time", TimestampType(), True)      #datetime of the last element of the current batch (take care of the window aspect in db)
    ])

schema_classif = StructType([
    StructField("sensor_type",IntegerType(), True),
    StructField("time",TimestampType(), True),
    StructField("slot",IntegerType(), True),
    StructField("p-i",StringType(), True),
    StructField("measurement",FloatType(), True),
    StructField("voltage",FloatType(), True)
    ])

schema_global = StructType([
    StructField("sensor_type",IntegerType(), True),
    StructField("time",TimestampType(), True),
    StructField("p-i",StringType(), True),
    StructField("measurement",FloatType(), True),
    StructField("voltage",FloatType(), True)
    ])


rows_slots = filestream.flatMap(parseRowTimeSlots).filter(lambda r: int(r["p-i"].split("-")[1]) == 0)	#2nd query

rows_slots.foreachRDD(lambda rdd: storeRdd(rdd, my_spark, schema_classif, timeslots))

rows = filestream.flatMap(parseRow)		#1st & 3rd queries
rows.foreachRDD(lambda rdd: storeRdd(rdd, my_spark, schema_global, statistics))



window_time = 60*60 #one hour
window_slide = 30   #30sec
rows_classif = rows.filter(lambda r: int(r["p-i"].split("-")[1]) == 0)      #Get only the temperature sensors

last_time = rows_classif.map(lambda r:(r["time"])).reduce(lambda r1, r2: max(r1, r2))       #Last time object of the batch


num_per_value = rows_classif.map(lambda r: (r["measurement"])).countByValue().window(window_time,window_slide)  #Last hour, window slide = 15sec


num_distinct_value = num_per_value.count() 

total_count = rows_classif.count().window(window_time,window_slide).reduce(add)        #Get the total number of record within the window -> to compute relative frequencies

#Store (measurement, frequency, total number of measurementw in window)
freq_per_value = num_per_value.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),total_count)\
                                .map(lambda r: (r[0][0], r[0][1]/r[1], r[1]))

#Store (measurement, frequency, distinct number of measurements in window)
freq_per_value_and_counts = freq_per_value.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),num_distinct_value)\
                                .map(lambda r: (r[0][0], r[0][1], r[1]))

top_freq = freq_per_value_and_counts.transform(lambda rdd: rdd.sortBy(lambda r: r[1], False)).filter(lambda r: r[1] > 5/r[2])

top_freq_date = top_freq.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),last_time)\
                                .map(lambda r: (r[0][0], r[0][1], r[1]))

top_freq_date.foreachRDD(lambda rdd: storeRdd(rdd, my_spark, schema_window, window_collection))

sc.setCheckpointDir("checkpoint")

ssc.start()
ssc.awaitTermination()