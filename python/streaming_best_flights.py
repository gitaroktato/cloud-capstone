#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
import sys

def printResults(rdd):
    """
    Print partial results to screen.
    """
    print "----------------- SNAPSHOT ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

def AMOrPM(departureTime):
	depTimeHours = int(departureTime[:2])
	if depTimeHours < 12:
		return 'AM'
	else:
		return 'PM'

def departureTimePretty(departureTime):
	depTimeHours = departureTime[:2]
	depTimeMinutes = departureTime[2:4]
	return '%s:%s' % (depTimeHours, depTimeMinutes)


def getMinimum(newValues, currentMin):
    if currentMin is None:
        currentMin = newValues[0]
	# Get minimum from all
	newValues.append(currentMin)
	return min(newValues, key=lambda item: item[3])

# TODO 2 Kafka

# MAIN

sc = SparkContext(appName="BestFlights")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint-best-flights")
lines = KafkaUtils.createDirectStream(ssc, ['input_2008'], \
	{"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Filter only for data in 2008
lines = lines.map(lambda tup: tup[1])

# Split each line by separator
rows = lines.map(lambda line: line.split())

# Get relevant data
rows = rows.filter(lambda row: len(row) > 8)
airports_fromto = rows.map(lambda row: ( \
		(row[0], row[1], row[2], AMOrPM(row[5])), \
		(row[3], row[4], departureTimePretty(row[5]), float(row[8])) \
	) \
)
airports_fromto = airports_fromto.updateStateByKey(getMinimum)
# TODO Fitlers

# Print and save
airports_fromto.foreachRDD(printResults)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

