#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter
import sys

def printResults(rdd):
    print "----------------- SAMPLE ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

# MAIN

sc = SparkContext(appName="Peek")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
lines = KafkaUtils.createDirectStream(ssc, [sys.argv[2]], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Peek
lines.foreachRDD(printResults)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

