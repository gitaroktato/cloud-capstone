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

sc = SparkContext(appName="TopTenCarriers")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("s3a://cloudcapstone-checkpoints/checkpoints/checkpoint-top-carriers")
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
rows = rows.filter(lambda row: len(row) > 8)
carriers = rows.map(lambda row: (row[3], float(row[8])))

# Count averages
carriers = carriers.updateStateByKey(updateFunction)

# Filter top ten
sorted = carriers.map(lambda tuple: (tuple[1][2], (tuple[0], tuple[1][1])))
# We filter at each worker by partition as well, reducing shuffling time between each workers
sorted = sorted.transform(lambda rdd: rdd.mapPartitions(cutOffTopTen))
# Final sorting
sorted = sorted.transform(lambda rdd: rdd.sortByKey())
# Saving and debugging
sorted.foreachRDD(lambda rdd: printResults(rdd))
sorted.foreachRDD(lambda rdd: saveResults(rdd))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
