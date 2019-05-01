from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import os
from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pandas as pd

STREAM_IN = "stream-IN"
print("Deleting existing files in %s ..." % STREAM_IN)
p = Path('.') / STREAM_IN
for f in p.glob("*.tmp"):
    os.remove(f)
print("... done")
sc = SparkContext("local[*]", "test")
sc.setLogLevel("WARN")   #Make sure warnings and errors observed by spark are printed.
#text_file = sc.textFile("stream-IN/sensor_type-0_0.tmp")
ssc = StreamingContext(sc, 5)  #generate a mini-batch every 5 seconds

filestream = ssc.textFileStream(STREAM_IN) #monitor new files in folder stream-IN
#print(filestream)
#text_file = sc.textFile("hdfs://stream-IN/sensor_type-0_0.tmp")

spark = SparkSession.builder.appName("Dash").getOrCreate()

stats = sc.textFile("data/db/stats.txt")
statistics = stats.map(lambda p: Row(time=p[0], min=p[1], max=p[2], avg=p[3]))

schemaString = "time min max avg"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.createDataFrame(statistics, schema)#.show()
df.show()
curr_df = df.toPandas()
curr_df.to_csv("data/db/dayum.csv")
#df.select("time", "min", "max", "avg").write.save("data/db/new_stats.json", format="json")

other_df = pd.DataFrame([[1, 2, 3, 4]], columns = ["time", "min", "max", "avg"])
curr_df = curr_df.append(other_df, ignore_index=True)
print(curr_df)

with open("data/db/dayum.csv", 'a') as f:
    curr_df.to_csv(f, header=False)

def parseRow(row):
    '''parses a single row into a dictionary'''
    #print("test")

    try:
        v = row.split(" ")
        return [{"topic": int(v[0]),
                 "time": datetime.strptime(v[1] + " "+ v[2], "%Y-%m-%d %H:%M:%S.%f"),
                 "p-i": v[3],
                 "measurement": float(v[4]),
                 "voltage": float(v[5]),
                "municipality": v[6]}]
    except Exception as err:
        print("Unexpected error: %s" % (err))


def checkAttributes(space_tag, time_tag):

    if space_tag < 0 or space_tag > 2:
        print("Space tag value is not supported. Give a value in [0,2].")
        print("0: grouped per space")
        print("1: grouped per municipality")
        print("2: grouped for the entirety of Brussels")
        exit()
        #return False
    elif time_tag <0 or time_tag > 4:
        print("Time tag value is not supported. Give a value in [0,4].")
        print("0: last 24h")
        print("1: last 2 days")
        print("2: last week")
        print("3: last month")
        print("4: last year")
        exit()
        #return False
    else:
        return True


def updateMax(current_max, state_max):
    """
    current_max: record containing the maximum value in the current batch
    state_max: float value being the maximum value seen so far
    """

    if len(current_max) == 0:       #If we did not read anything
        if state_max is None:       #If the state is not computed yet
            return None
        else:
            return state_max
    if state_max is None:
        return current_max[0]["measurement"]
    else:
        return max(current_max[0]["measurement"], state_max)

def updateMin(current_min, state_min):

    if len(current_min) == 0:
        if state_min is None:
            return None
        else:
            return state_min
    if state_min is None:
        return current_min[0]["measurement"]
    else:
        return min(current_min[0]["measurement"], state_min)

def updateMean(current, state):

    if len(current) == 0:       #Did not read anything from the stream (batch empty)
        if state is None or state is (None, None):      #if state was not initialized yet OR was already initialized with empty batch
            return (None, None)
        else:
            return state

    current_mean, current_count = current[0][0], current[0][1]
    if state is None or state is (None,None):      #If we read something from the stream for the first time
        return (current_mean, current_count)
    else:                                               #Update the total avg
        state_mean, state_count = state[0], state[1]
        total_count = current_count + state_count
        return ((current_mean * current_count + state_mean *state_count)/total_count, total_count)

def basicStats(space_tag, time_tag):
    checkAttributes(space_tag, time_tag)
    #filestream = ssc.textFileStream(STREAM_IN)
    #print(filestream)
    rows = filestream.flatMap(parseRow)
    #rows = text_file.map(lambda r: parseRow(r))
    #rows = text_file.flatMap(parseRow)
    #rows.pprint()
    #print(rows.pprint())
    if space_tag == 0: #Group per place
        group_space = rows.map(lambda r: ((r["topic"],r["p-i"].split("-")[0]), r))
        #key = (sensor_type, place), value = row

    elif space_tag == 1: #Group per municipality
        group_space = rows.map(lambda r: ((r["topic"],r["municipality"]), r))
        #key = (sensor_type, municipality), value = row

    else:                #Group for Brussels
        group_space = rows.map(lambda r: ((r["topic"]), r))
        #key = (sensor_type), value = row
    #print(group_space.pprint())
    #current_time = datetime.now()   #Ideally

    #tmp_time = rows.map(lambda r: ("time", r["time"]))
    #tmp_time.pprint()
    #current_time = tmp_time.reduceByKey(lambda r1, r2: max(r1, r2, key=lambda r: r["measurement"]))
    #current_time.pprint()
    current_time = datetime.strptime("2017-02-28 00:09:19.196", "%Y-%m-%d %H:%M:%S.%f") #As in stream_to_kafka file
    if time_tag == 0:   #Last 24h
        window_time = 60*60*24

    elif time_tag == 1:  #Last 48h
        window_time = 60*60*48

    elif time_tag == 2:  #Last week
        window_time = 60*60*24*7

    elif time_tag == 3:  #Last month
        window_time = 60*60*24*30.4375

    else:                #Last year
        window_time = 60*60*24*365.25


    max_by_group_batch = group_space.reduceByKey(lambda r1, r2: max(r1, r2, key=lambda r: r["measurement"])).window(window_time, 10)
    min_by_group_batch = group_space.reduceByKey(lambda r1, r2: min(r1, r2, key=lambda r: r["measurement"])).window(window_time, 10)
    #We get basic stats for each batch within the window


    max_by_group_window = max_by_group_batch.reduceByKey(lambda r1, r2: max(r1, r2, key=lambda r: r["measurement"])).map(lambda r: (r[0],r[1]["measurement"]))
    min_by_group_window = min_by_group_batch.reduceByKey(lambda r1, r2: min(r1, r2, key=lambda r: r["measurement"])).map(lambda r: (r[0],r[1]["measurement"]))

    #Get the basic stats from the selected elements from the batchs that are in the window



    mean_by_group_window = group_space.transform(lambda rdd: rdd.aggregateByKey((0,0),
                    lambda acc1, value: (acc1[0] + value["measurement"]   , acc1[1] + 1   ),
                    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) )\
                    .mapValues(lambda v: (v[0]/v[1], v[1])) \
                    .sortBy(lambda pair: pair[1], False)
                    ).window(window_time,10).reduceByKey(lambda r1, r2: ( (r1[0]*r1[1]+r2[0]*r2[1])/(r1[1]+r2[1]), (r1[1]+r2[1]) ))



    max_by_group_window.pprint()
    min_by_group_window.pprint()
    mean_by_group_window.pprint()
    # set the spark checkpoint
    # folder to the subfolder of the current folder named "checkpoint"
    sc.setCheckpointDir("checkpoint")
    ssc.start()
    ssc.awaitTermination()


basicStats(2,0)
#volumePerClient = orders.map(lambda o: (o['clientId'], o['amount'] * o['price']))
#volumeState = volumePerClient.updateStateByKey(lambda vals, totalOpt: sum(vals) + totalOpt if totalOpt != None else sum(vals))
