#!/usr/bin/python

#Usage: 
#spark-submit --jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar /u01/workspace/02_avg.py localhost:9092 sql-insert1

from __future__ import print_function
import sys
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from operator import add
import json

def saveCoord(rdd):
    rdd.foreach(lambda rec: open("/tmp/spark_test.csv", "a").write(str(rec[0])+","+str(rec[1][0])+","+str(rec[1][1])+"\n"))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="IoT")
    ssc = StreamingContext(sc, 3)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    jsonRDD = kvs.map(lambda x: json.loads(x[1]))

   ##### Processing #####

    # Average temperature in each state 
    avgval = jsonRDD.map(lambda x: (x['DT_HR_HH'],(x['VAL'],1))) \
                 .reduceByKey(lambda a,b: (float(a[0])+float(b[0]), a[1]+b[1]))
    sortedval = avgval.transform(lambda x: x.sortBy(lambda y: y[0], True))
    sortedval.foreachRDD(saveCoord)
    ssc.start()
    ssc.awaitTermination()

