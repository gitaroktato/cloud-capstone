#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql import SQLContext, SparkSession
import sys

def printResults(rdd):
    print "----------------- SAMPLE ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def saveToCassandra(rdd):
	if rdd.count() == 0:
		return
	# Get the singleton instance of SparkSession
	spark = getSparkSessionInstance(rdd.context.getConf())

	rowRdd = rdd.map(lambda row: Row(airport=row[0], airport_to=row[1], arr_delay=row[2]))
	df = spark.createDataFrame(rowRdd)
	df.write\
		.format("org.apache.spark.sql.cassandra")\
		.mode('append')\
		.options(table="airport_airport_arrival", keyspace="aviation")\
		.save()

# MAIN

sc = SparkContext(appName="AirportsAirportsArrivalToCassandra")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
lines = KafkaUtils.createDirectStream(ssc, ['airports_airports_arrival'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Transform
lines = lines.map(lambda message: message[1])
lines = lines.map(lambda line: line.split())
lines = lines.map(lambda tuple: (tuple[0], tuple[1], float(tuple[2])))
# Save to Cassandra
lines.foreachRDD(printResults)
lines.foreachRDD(saveToCassandra)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
