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
	
	rowRdd = rdd.map(lambda row: Row(\
		airport_from=row[0], \
		airport_to=row[1], \
		given_date=row[2], \
		am_or_pm=row[3], \
		carrier=row[4], \
		flight_num=row[5], \
		departure_time=row[6], \
		arr_delay=row[7]\
	))
	df = spark.createDataFrame(rowRdd)
	df.write\
		.format("org.apache.spark.sql.cassandra")\
		.mode('append')\
		.options(table="best_flights_2008", keyspace="aviation")\
		.save()

# MAIN

sc = SparkContext(appName="BestFlights2008")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
lines = KafkaUtils.createDirectStream(ssc, ['best_flights_2008'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Transform
lines = lines.map(lambda message: message[1])
lines = lines.map(lambda line: line.split())
lines = lines.map(lambda record: tuple(record[0:7]) + (float(record[7]),))
# Save to Cassandra
lines.foreachRDD(printResults)
lines.foreachRDD(saveToCassandra)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

