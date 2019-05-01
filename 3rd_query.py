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
        return [{"topic": int(v[0]),
                 "time": datetime.strptime(v[1] + " "+ v[2], "%Y-%m-%d %H:%M:%S.%f"),
                 "p-i": v[3],
                 "measurement": round(float(v[4]),1),       #rounded precision to 1 digit (follow the requirements)
                 "voltage": float(v[5]),
                "municipality": v[6]}]
    except Exception as err:
        print("Unexpected error: %s" % (err))



window_time = 60*60 #one hour
window_slide = 15   #15sec
freq_tresh = 1./15
rows = filestream.flatMap(parseRow).filter(lambda r: int(r["p-i"].split("-")[1]) == 0)      #Get only the temperature sensors

num_per_value = rows.map(lambda r: (r["measurement"])).countByValue().window(window_time,window_slide)  #Last hour, window slide = 15sec
#num_per_value.pprint(num=60)
total_count = rows.count().window(window_time,window_slide).reduce(add)        #Get the total number of record within the window -> to compute relative frequencies
#total_count.pprint()
freq_per_value = num_per_value.transformWith(lambda rdd1, rdd2: rdd1.cartesian(rdd2),total_count)\
                                .map(lambda r: (r[0][0], r[0][1]/r[1]))
#freq_per_value.pprint()
top_freq = freq_per_value.transform(lambda rdd: rdd.sortBy(lambda r: r[1], False)).filter(lambda r: r[1] > freq_tresh)

top_freq.pprint()

sc.setCheckpointDir("checkpoint")
ssc.start()
ssc.awaitTermination()