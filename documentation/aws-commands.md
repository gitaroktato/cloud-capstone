# AWS commands
```
nohup ~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master local[4] --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=4000 ./ingest_files_to_kafka.py input
```

# Instance list
ingest c3.xlarge 40 GB + 15 GB read-only
kafka [2] t2.medium 16 GB
spark [3] t2.medium 8 GB
zookeeper + kafka-manager t2.small 8 GB
S3 bucket for checkpoints 
