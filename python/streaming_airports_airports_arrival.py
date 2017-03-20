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

def saveResults(rdd):
	"""
	Save results as a report.
	"""
	results = rdd.collect();
	if len(results) == 0:
		return
	file = open(sys.argv[2], 'w')
	for item in results:
		file.write("\n%s -> %s: %s\n\n" % (item[0][0], item[0][1], item[1]))

def updateFunction(newValues, runningAvg):
    if runningAvg is None:
        runningAvg = (0.0, 0, 0.0)
    # calculate sum, count and average.
    prod = sum(newValues, runningAvg[0])
    count = runningAvg[1] + len(newValues)
    avg = prod / float(count)
    return (prod, count, avg)

def sendToKafka(records):
	"""
	Send records to Kafka. The format is the following
	JFK LAX -3.013333
	JFK ORD 0.01113
	"""
	kafka = KafkaClient('172.31.62.92:9092,172.31.55.234:9092')
	producer = SimpleProducer(kafka)
	for record in records:
		message = "%s %s %s" % (record[0][0], record[0][1], record[1])
		producer.send_messages('airports_airports_arrival', message.encode())

# MAIN

sc = SparkContext(appName="AirportAirportArrival")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("s3a://cloudcapstone-checkpoints/checkpoints/checkpoint-airport-airport-arrival")
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
rows = rows.filter(lambda row: len(row) > 8)
airports_fromto = rows.map(lambda row: ((row[0], row[1]), float(row[8])))

# Count averages
airports_fromto = airports_fromto.updateStateByKey(updateFunction)
# Change key to just airports
airports = airports_fromto.map(lambda row: ((row[0][0], row[0][1]), row[1][2]))
#Filter and print
airports = airports.filter(lambda x: x[0] in [('LGA','BOS'),('BOS','LGA'),('OKC','DFW'),('MSP','ATL')])

airports.foreachRDD(printResults)
airports.foreachRDD(saveResults)

# Kafka sink
airports.foreachRDD(lambda rdd: rdd.foreachPartition(sendToKafka))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
