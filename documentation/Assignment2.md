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

# Peek to topic
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=125  python/streaming_peek_to_topic.py localhost:9092 input
```

# Ingest with pyspark
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master local[4] --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=4000 ./ingest_files_to_kafka.py input
```

# Top 10 airports
```
 ~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --conf spark.streaming.kafka.maxRatePerPartition=125000 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3  ./streaming_top_airports.py 172.31.62.92:9092,172.31.55.234:9092 topten_airports.log
```

# TOP 10 carriers
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=125000  ./streaming_top_carriers.py 172.31.62.92:9092,172.31.55.234:9092 topten_carriers.log
```

# Question 2.1
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=125000  ./streaming_top_carriers_by_airports.py 172.31.62.92:9092,172.31.55.234:9092 top_carriers_by_airports.log
```

# Question 2.1 to Cassandra
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=250 --conf spark.cassandra.connection.host=172.31.51.216 ./streaming_top_carriers_by_airports_to_cassandra.py 172.31.62.92:9092,172.31.55.234:9092
```

# Question 2.2
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=250000  ./streaming_top_airports_by_airports.py 172.31.62.92:9092,172.31.55.234:9092 top_airports_by_airports.log
```

# Question 2.2 to Cassandra
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11 --conf spark.streaming.kafka.maxRatePerPartition=2500 --conf spark.cassandra.connection.host=localhost python/streaming_top_airports_by_airports_to_cassandra.py localhost:9092
```

# Question 2.4
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=250000  python/streaming_airports_airports_arrival.py localhost:9092 airports_airports_arrival.log
```

# Question 2.4 to Cassandra
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11 --conf spark.streaming.kafka.maxRatePerPartition=2500 --conf spark.cassandra.connection.host=localhost  python/streaming_airports_airports_arrival_to_cassandra.py localhost:9092
```

# Question 3.2
Filter for 2008
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=250000  python/streaming_filter_2008.py localhost:9092
```

# Optimizations
Calculating top ten on partition and aggregate results on the director

Cutting of unnecessary data in input topic and using it as staging area

Aggregating top ten carriers for each airport to reduce data transfer and cassandra save time (not all airport-carrier pairs will be stored in DB)
