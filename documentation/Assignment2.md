# Configuring kafka and ZK
Change basedir on BOTH from /tmp
Change retention policy on Kafka
Add delete.enabled on Kafka

# Starting Spark in distributed mode
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://sniper-VirtualBox:7077  python/streaming_top_airports.py stream result.log
```
```
sbin/start-master.sh
sbin/start-slave.sh
```
# Starting Kafka and manager
```
EXPORT ZK_HOSTS=localhost:2181
cd ~/kafka-manager-1.3.3.1/target/universal/kafka-manager-1.3.3.1
./bin kafka-manager
...
```
# PySpark with Kafka
```
sudo pip install kafka
```
# Truncate topic in Kakfka
```
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test
```

# Ingest with pyspark
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit  python/ingest_files_to_kafka.py stream localhost:9092
```

# Top 10 airports
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0  python/streaming_top_airports.py localhost:9092 result.log
```
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=125000  python/streaming_top_airports.py localhost:9092 result.log
```

# TOP 10 carriers
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=125000  python/streaming_top_carriers.py localhost:9092 topten_carriers.log
```

# Question 2.1
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=250000  python/streaming_top_carriers_by_airports.py localhost:9092
```

# Optimizations
Calculating top ten on partition and aggregate results on the director

Cutting of unnecessary data in input topic and using it as staging area

Aggregating top ten carriers for each airport to reduce data transfer and cassandra save time (not all airport-carrier pairs will be stored in DB)


