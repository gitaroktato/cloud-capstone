
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



# Optimizations

