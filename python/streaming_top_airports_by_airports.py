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
    for line in rdd.collect():
        print line
    print "SIZE: %d" % rdd.count()

def saveResults(rdd):
	"""
	Save results as a report.
	"""
	results = rdd.collect();
	if len(results) == 0:
		return
	file = open(sys.argv[2], 'w')
	for item in results:
		file.write("\n--- %s ---\n\n" % item[0])
		file.writelines(["(%s: %s)\n" % (record[0], record[1]) for record in item[1]])

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

def append(aggr, newAirportAvg):
	"""
	Add new element to aggregate. Aggregate contains top ten airports and departure delays.
	Sample: [('JFK',-0.0001), ('SFO',0.025), ('LAX',0.3)]
	"""
	aggr.append(newAirportAvg)
	aggr.sort(key=lambda element: element[1])
	return aggr[0:10]

def combine(left, right):
	"""
	Combine two aggregates. Aggregate contains top ten airports and departure delays.
	Sample: [('JFK',-0.0001), ('SFO',0.025), ('LAX',0.3)]
	"""
	for newElement in right:
		left.append(newElement)
	left.sort(key=lambda element: element[1])
	return left[0:10]


def sendToKafka(records):
	"""
	Send records to Kafka. The format is the following
	JFK LAX -3.013333
	JFK ORD 0.01113
	"""
	kafka = KafkaClient('172.31.62.92:9092,172.31.55.234:9092')
	producer = SimpleProducer(kafka)
	for record in records:
		for item in record[1]:
			message = "%s %s %s" % (record[0], item[0], item[1])
			producer.send_messages('top_airports_by_airports', message.encode())

# MAIN

sc = SparkContext(appName="TopTenAirportsByAirports")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("s3a://cloudcapstone-checkpoints/checkpoints/checkpoint-top-airports-by-airports")
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
rows = rows.filter(lambda row: len(row) > 7)
airports_and_carriers = rows.map(lambda row: ((row[0], row[1]), float(row[7])))

# Count averages
airports_and_carriers = airports_and_carriers.updateStateByKey(updateFunction)
# Change key to just airports
airports = airports_and_carriers.map(lambda row: (row[0][0], (row[0][1], row[1][2])))
# Aggregate to just top 10 carriers
airports = airports.transform(lambda rdd: rdd.aggregateByKey([],append,combine))
#Filter and print
airports = airports.filter(lambda x: x[0] in ['SRQ', 'CMH', 'JFK', 'SEA', 'BOS'])

airports.foreachRDD(printResults)
airports.foreachRDD(saveResults)

# Kafka sink
airports.foreachRDD(lambda rdd: rdd.foreachPartition(sendToKafka))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
