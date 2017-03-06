#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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


sc = SparkContext("local[2]", "TopTenAirports")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 10)
lines = ssc.textFileStream(sys.argv[1])

# Split each line by separator
rows = lines.map(lambda line: line.split(',')).filter(lambda l: len(l) > 18)

# Get the airports
airports = rows.flatMap(lambda row: [row[11], row[18]])
airports = airports.map(lambda airport: airport.replace('"', '').encode('ascii','ignore'))
airports = airports.filter(lambda key: len(key) == 3)

# Count them
airportsCounted = airports.map(lambda airport: (airport, 1)).reduceByKey(lambda x, y: x + y)

# Filter top ten
sorted = airportsCounted.map(lambda tuple: (tuple[1], tuple[0])).transform(lambda rdd: rdd.sortByKey(False))
sorted.foreachRDD(lambda rdd: printResults(rdd))
sorted.foreachRDD(lambda rdd: saveResults(rdd))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

