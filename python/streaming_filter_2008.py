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

def sendToKafka(lines):
	"""
	Send lines to Kafka.
	"""
	kafka = KafkaClient('localhost:9092')
	producer = SimpleProducer(kafka)
	for line in lines:
		producer.send_messages('input_2008', line.encode())

# MAIN

sc = SparkContext(appName="FilterFor2008")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Filter for just data in 2008
lines = lines.map(lambda tup: tup[1]).filter(lambda line: '2008-' in line)

# Print
lines.foreachRDD(printResults)

# Kafka sink
lines.foreachRDD(lambda rdd: rdd.foreachPartition(sendToKafka))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

