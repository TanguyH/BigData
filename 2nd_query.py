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

"""

time = datetime.strptime("2017-02-28 00:15:00.000", "%Y-%m-%d %H:%M:%S.%f")
day = time.date()
slot = time.time()
print("datetime : ", time)
print("hour: ",slot.hour)
print("minute: ",slot.minute)
print("second: ",slot.second)
print("microsecond: ",slot.microsecond)
if slot.minute % 15 == 0 and slot.second == 0 and slot.microsecond == 0:
    chosen_slot = 4*slot.hour + slot.minute//15 - 1
else:
    chosen_slot = 4*slot.hour + slot.minute//15
print("fall in slot : ",chosen_slot)

"""

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
        return [{"topic": int(v[0]),
                "time": date_time,
                "day": day,
                "slot": slot,
                "p-i": v[3],
                "voltage": float(v[5]),
                "measurement":float(v[4]),
                "municipality": v[6],
                "type_space": v[7]}]

    except Exception as err:
        print("Unexpected error: %s" % (err))


def checkAttributes(space_tag, type_space_tag, time_tag):

    if space_tag < 0 or space_tag > 2:
        print("Space tag value is not supported. Give a value in [0,2].")
        print("0: grouped per space")
        print("1: grouped per municipality")
        print("2: grouped for the entirety of Brussels")
        exit()

    elif type_space_tag < 0 or type_space_tag > 1:
        print("Type_space tag value is not supported. Give a value in [0,1].")
        print("0: public place")
        print("1: private place")
        exit()

    elif time_tag <0 or time_tag > 2:
        print("Time tag value is not supported. Give a value in [0,2].")
        print("0: last week")
        print("1: last month")
        print("2: last year")
        exit()
    else:
        return True


def classification(space_tag, type_space_tag, time_tag):
    checkAttributes(space_tag, type_space_tag, time_tag)


    rows = filestream.flatMap(parseRow).filter(lambda r: int(r["p-i"].split("-")[1]) == 0)      #Get only the temperature sensors
    mapped_rows = rows.map(lambda r: ((r["day"],r["slot"], int(r["p-i"].split("-")[0]), r["municipality"],r["type_space"]), r))#.reduceByKey(add)

    avg_per_class = mapped_rows.transform(lambda rdd: rdd.aggregateByKey((0,0),
                        lambda acc1, value: (acc1[0] + value["measurement"]   , acc1[1] + 1   ),
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) )\
                        .mapValues(lambda v: (v[0]/v[1])) \
                        .sortBy(lambda pair: pair[1], False)
                        )

    avg_per_class.pprint()

    slot_day_night = avg_per_class.filter(lambda r: r[1] < 19.5).map(lambda r: (r[0], (r[1], "night")))
    slot_day_day = avg_per_class.filter(lambda r: r[1] >= 19.5).map(lambda r: (r[0], (r[1], "day")))
    #You are hence asked
    #to compute, for each day d, each slot s, and each temperature sensor x, the average
    #temperature measured by x during slot s on day d, and use this to classify the slot s of
    #day d into day or night. )
    slot_day_night.pprint()
    slot_day_day.pprint()

    #    mean_by_group_window = group_space.transform(lambda rdd: rdd.aggregateByKey((0,0),
    #                    lambda acc1, value: (acc1[0] + value["measurement"]   , acc1[1] + 1   ),
    #                    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]) )\
    #                    .mapValues(lambda v: (v[0]/v[1], v[1])) \
    #                    .sortBy(lambda pair: pair[1], False)
    #                    )

    #sc.setCheckpointDir("checkpoint")
    ssc.start()
    ssc.awaitTermination()


classification(0,0,0)