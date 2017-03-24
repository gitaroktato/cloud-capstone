# Video playbook
## Deployment scheme, AWS instances
1. AWS Console
1. Kafka manager
1. Spark cluster

## Ingesting and analyzing the data
1. Show files in file system
1. Show code for consuming from local FS
1. Peek to input and input_2008 topics
```bash
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --conf spark.streaming.kafka.maxRatePerPartition=25  ./streaming_peek_to_topic.py 172.31.62.92:9092,172.31.55.234:9092 input_2008
```
1. Show Cassandra table definitions

## Querying results for each question
1. Show code for 1.1
1. Execute 1.1
```bash
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --conf spark.streaming.kafka.maxRatePerPartition=250 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3  ./streaming_top_airports.py 172.31.62.92:9092,172.31.55.234:9092 topten_airports.log
```

1. Show code for 2.1
1. Execute 2.1
```bash
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=250  ./streaming_top_carriers_by_airports.py 172.31.62.92:9092,172.31.55.234:9092 top_carriers_by_airports.log
```
1. Show results in Cassandra
```sql
select * from airport_carrier_departure where airport = 'CMH';
select * from airport_carrier_departure where airport = 'SEA';
```

1. Show code for 2.4
1. Execute 2.4
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=25000  ./streaming_airports_airports_arrival.py 172.31.62.92:9092,172.31.55.234:9092 airports_airports_arrival.log
```
1. Show results in Cassandra
```sql
select * from airport_airport_arrival where airport = 'OKC' and airport_to='DFW';
select * from airport_airport_arrival where airport = 'BOS' and airport_to='LGA';
```

1. Show code for 3.2
1. Execute 3.2
```
~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master spark://ip-172-31-49-121.ec2.internal:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.hadoop:hadoop-aws:2.7.3 --conf spark.streaming.kafka.maxRatePerPartition=125000  ./streaming_best_flights.py 172.31.62.92:9092,172.31.55.234:9092 best_flights_2008.log
```
1. Show results in Cassandra
```
select * from best_flights_2008;
```
