#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

def printResults(rdd):
    print "----------------- TOP 10 ----------------------"
    for line in rdd.take(10):
        print line

def saveResults(rdd):
    topten = rdd.take(10)
    if len(topten) == 0:
		return
    file = open(sys.argv[2], 'w')
    file.writelines(["%s  %s\n" % (item[1], item[0])  for item in topten])

# TODO
def cutOffTopTen(iterable):
	topTen = list()
	for tuple in iterable:
		if len(topTen) < 10:
			topTen.push(tuple)
			topTen = sorted(topTen, lambda element: element[0])
		elif topTen[0][0] < tuple[0]:
			topTen[0] == tuple
			topTen = sorted(topTen, lambda element: element[0])
    #return iter(topTen)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    # add the new values with the previous running count to get the new count
    return sum(newValues, runningCount)

sc = SparkContext("local[2]", "TopTenAirports")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint-top-airports")
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
airports = rows.flatMap(lambda row: [row[0], row[1]])

# Count them
airportsCounted = airports.map(lambda airport: (airport, 1)).updateStateByKey(updateFunction)

# Filter top ten
# We filter at each worker by partition as well, reducing shuffling time between each workers
sorted = airportsCounted.map(lambda tuple: (tuple[1], tuple[0]))
sorted = sorted.transform(lambda rdd: rdd.sortByKey(False))
sorted.foreachRDD(lambda rdd: printResults(rdd))
sorted.foreachRDD(lambda rdd: saveResults(rdd))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

