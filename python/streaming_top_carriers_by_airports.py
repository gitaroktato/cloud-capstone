#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter
import sys

def printResults(rdd):
    print "----------------- TOP 10 ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

def saveResults(rdd):
    topten = rdd.take(10)
    if len(topten) == 0:
		return
    file = open(sys.argv[2], 'w')
    file.writelines(["%s  %s\n" % (item[1], item[0])  for item in topten])

def cutOffTopTen(iterable):
	topTen = []
	for tupl in iterable:
		if len(topTen) < 10:
			topTen.append(tupl)
			topTen.sort()
		elif topTen[9][0] > tupl[0]:
			topTen[9] = tupl
			topTen.sort()
	return iter(topTen)

def updateFunction(newValues, runningAvg):
    if runningAvg is None:
        runningAvg = (0.0, 0, 0.0)
    # calculate sum, count and average.
    prod = sum(newValues, runningAvg[0])
    count = runningAvg[1] + len(newValues)
    avg = prod / float(count)
    return (prod, count, avg)

def append(aggr, newCarrierAvg):
	"""
	Add new element to aggregate. Aggregate contains top ten carriers and departure delays.
	Sample: [('TZ',-0.0001), ('AQ',0.025), ('MS',0.3)]
	"""
	aggr.append(newCarrierAvg)
	aggr.sort(key=lambda element: element[1])
	return aggr[0:9]

def combine(left, right):
	"""
	Combine two aggregates. Aggregate contains top ten carriers and departure delays.
	Sample: [('TZ',-0.0001), ('AQ',0.025), ('MS',0.3)]
	"""
	for newElement in right:
		left.append(newElement)
	left.sort(key=lambda element: element[1])
	return left[0:10]

sc = SparkContext("local[2]", "TopTenCarriers")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint-top-carriers-by-airports")
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
rows = rows.filter(lambda row: len(row) > 7)
airports_and_carriers = rows.map(lambda row: ((row[0], row[3]), float(row[7])))

# Count averages
airports_and_carriers = airports_and_carriers.updateStateByKey(updateFunction)
# Change key to just airports
airports = airports_and_carriers.map(lambda row: (row[0][0], (row[0][1], row[1][2])))
# Aggregate to just top 10 carriers
airports = airports.transform(lambda rdd: rdd.aggregateByKey([],append,combine))
#Filter and print
airports = airports.filter(lambda x: x[0] in ['SRQ', 'CMH', 'JFK', 'SEA', 'BOS'])
airports.foreachRDD(printResults)


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

