#!/usr/bin/python
"""
This script will ingest files from a certain folder and put it into input topic in Kafka.
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
import sys

def printResults(rdd):
    print "----------------- SAMPLE ----------------------"
    for line in rdd.take(10):
        print line

def sendToKafka(messages):
    kafka = KafkaClient('172.31.62.92:9092,172.31.55.234:9092')
    producer = SimpleProducer(kafka, async=False)
    for message in messages:
        producer.send_messages('input', message.encode())

sc = SparkContext(appName="IngestFilesToKafka")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 3)
lines = ssc.textFileStream(sys.argv[1])

# Split each line by separator
lines = lines.map(lambda line: line.replace('"', ''))
rows = lines.map(lambda line: line.split(',')).filter(lambda l: len(l) > 38)
# Drop garbage
rows = rows.filter(lambda row: len(row[11]) == 3 and len(row[18]) == 3)

# Get only the necessary fields
records = rows.map(lambda row: " ".join((row[11], row[18], row[5], row[8], row[10], row[25], row[26], row[27], row[38])))

# Debug
records.foreachRDD(lambda rdd: printResults(rdd))

# Kafka sink
records.foreachRDD(lambda rdd: rdd.foreachPartition(sendToKafka))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

