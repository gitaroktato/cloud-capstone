# Dataset mount & initialization
AWS EBS mount details
```
lsblk
sudo mkdir /data
mount /dev/xvdb /data
```
[link](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-attaching-volume.html)
[link](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)

# Data exploration using bash
cat ./On_Time_On_Time_Performance_2008_10.zip | gunzip | head -255 > ~/airline_ontime_perf.csv

```
[ec2-user@ip-172-31-51-194 aviation]$ ls
air_carrier_employees          air_carrier_statistics_summary  airline_origin_destination    aviation_safety_reporting
air_carrier_financial_reports  air_carrier_statistics_US       aviation_accident_database    aviation_support_tables
air_carrier_statistics_ALL     airline_ontime                  aviation_accident_statistics  small_air_carrier
```

```
[root@ip-172-31-51-194 airline_ontime]# ls
1988  1989  1990  1991  1992  1993  1994  1995  1996  1997  1998  1999  2000  2001  2002  2003  2004  2005  2006  2007  2008
[root@ip-172-31-51-194 airline_ontime]# cd 2008
[root@ip-172-31-51-194 2008]# ls
On_Time_On_Time_Performance_2008_10.zip  On_Time_On_Time_Performance_2008_2.zip  On_Time_On_Time_Performance_2008_6.zip
On_Time_On_Time_Performance_2008_11.zip  On_Time_On_Time_Performance_2008_3.zip  On_Time_On_Time_Performance_2008_7.zip
On_Time_On_Time_Performance_2008_12.zip  On_Time_On_Time_Performance_2008_4.zip  On_Time_On_Time_Performance_2008_8.zip
On_Time_On_Time_Performance_2008_1.zip   On_Time_On_Time_Performance_2008_5.zip  On_Time_On_Time_Performance_2008_9.zip
```

```
[root@ip-172-31-51-194 aviation]# cd aviation_support_tables/
[root@ip-172-31-51-194 aviation_support_tables]# ls
Aircraft Types  Carrier Decode  Master Coordinate
```

# Mounting to VirtualBox
```
sudo mount -t vboxsf transportation /transportation
fdisk /dev/sdb
mkfs -t ext4 /dev/sdb
mount /dev/sdb
```

# Uploading data to HDFS
```
export HADOOP_HOME=~/hadoop-2.7.3
~/IdeaProjects/cloud-capstone/migration/move-to-hadoop.sh ~/aviation/ /user/sniper/origin_dest
```

# Loading data with hive
```
CREATE TABLE raw (line STRING);
LOAD DATA INPATH '/user/hive/warehouse/transportation/airline_from_to.zip' overwrite INTO TABLE raw;


CREATE TABLE raw (line STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

CREATE TABLE raw_sequence (line STRING)
   STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH '/user/hive/warehouse/transportation/airline_from_to.zip' INTO TABLE raw;

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK; -- NONE/RECORD/BLOCK (see below)
INSERT OVERWRITE TABLE raw_sequence SELECT * FROM raw;

```

# Raw mapreduce jobs for zipped formats.


# Spark and pyspark for zipped formats


# Deploying cluster to AWS
```
Core Hadoop: Hadoop 2.7.3 with Ganglia 3.7.2, Hive 2.1.1, Hue 3.11.0, Mahout 0.12.2, Pig 0.16.0, and Tez 0.8.4

HBase: HBase 1.2.3 with Ganglia 3.7.2, Hadoop 2.7.3, Hive 2.1.1, Hue 3.11.0, Phoenix 4.7.0, and ZooKeeper 3.4.9

Presto: Presto 0.152.3 with Hadoop 2.7.3 HDFS and Hive 2.1.1 Metastore

Spark: Spark 2.1.0 on Hadoop 2.7.3 YARN with Ganglia 3.7.2 and Zeppelin 0.6.2
```
# Data Cleaning
```
~/IdeaProjects/cloud-capstone/migration/move-ontime-perf-to-hadoop.sh ~/aviation/ /user/sniper/ontime_perf
```
# Hadoop job to execute
```
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.OnTimeArrivalByAirports ontime_perf departure_by_airports
```

# Getting top ten airports by Spark
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/popular_unranked_2/part-r-00000')
rdd = file.cache()
rdd.map(lambda line: line.split()).filter(lambda tuple: len(tuple) == 2).filter(lambda tuple: len(tuple[0]) == 3).map(lambda tuple: (int(tuple[1]), tuple[0])).sortByKey(ascending=False).take(10)
```

# Getting the top ten carriers by arrival time
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/avg_delays/part-r-00000')
rdd = file.cache()
rdd = rdd.map(lambda line: line.split()).cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[1]), tuple[0])).cache()
rdd2.takeOrdered(10)
```
# Airport: (Airline, Average delay in minutes)
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/departure_by_carriers/part-r-00000')
rdd = file.map(lambda line: line.split()).cache()
rdd = rdd.filter(lambda tuple: tuple[0] == 'CMI').cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0])).cache()
rdd2.takeOrdered(10)
```

# Saving to cassandra
```
./bin/pyspark --conf spark.cassandra.connection.host=127.0.0.1 --packages datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11

```
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/departure_by_carriers/part-r-00000')
rdd = file.map(lambda line: line.split())
rdd2 = rdd.map(lambda tuple: (tuple[0], tuple[1], float(tuple[2])))
from pyspark.sql import Row
rdd3 = rdd2.map(lambda row: Row(airport=row[0], carrier=row[1], dep_delay=row[2]))
df = spark.createDataFrame(rdd3)
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="airport_carrier_departure", keyspace="aviation")\
    .save()
```
# Airport: Average delay in minutes
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/departure_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split()).cache()
rdd = rdd.filter(lambda tuple: tuple[0] == 'CMI').cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0])).cache()
rdd2.takeOrdered(10)
```
# Saving to cassandra
```
./bin/pyspark --conf spark.cassandra.connection.host=127.0.0.1 --packages datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11

```
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/departure_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split())
rdd = rdd.filter(lambda tuple: len(tuple) == 3).filter(lambda tuple: len(tuple[0]) == 3).filter(lambda tuple: len(tuple[1]) == 3)
rdd2 = rdd.map(lambda tuple: (tuple[0], tuple[1], float(tuple[2])))
from pyspark.sql import Row
rdd3 = rdd2.map(lambda row: Row(airport=row[0], airport_to=row[1], dep_delay=row[2]))
df = spark.createDataFrame(rdd3)
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="airport_airport_departure", keyspace="aviation")\
    .save()
```
# Airport: arrival delay in minutes
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/arrival_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split())
rdd2 = rdd.filter(lambda tuple: tuple[0] == 'CMI' and tuple[1] == 'ORD')
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0]))
rdd2.collect()
```
# Saving to cassandra
```
./bin/pyspark --conf spark.cassandra.connection.host=127.0.0.1 --packages datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11

```
```
file = sc.textFile('hdfs://localhost:9000/user/sniper/arrival_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split())
rdd = rdd.filter(lambda tuple: len(tuple) == 3).filter(lambda tuple: len(tuple[0]) == 3).filter(lambda tuple: len(tuple[1]) == 3)
rdd2 = rdd.map(lambda tuple: (tuple[0], tuple[1], float(tuple[2])))
from pyspark.sql import Row
rdd3 = rdd2.map(lambda row: Row(airport=row[0], airport_to=row[1], arr_delay=row[2]))
df = spark.createDataFrame(rdd3)
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="airport_airport_arrival", keyspace="aviation")\
    .save()
```
# Travel planner for 2008
Data cleaning
```
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.BestFlightOnAGivenDate ontime_perf/*2008*.csv best_flights_2008
```
```
def get_best_flight(catalog, from_airport, to_airport, date, am_or_pm):
    line = catalog.filter(lambda line: from_airport in line
                                       and to_airport in line
                                       and date in line
                                       and am_or_pm in line).first()
    tuple = line.split()
    return tuple[4:]


data_catalog = 'hdfs://localhost:9000/user/sniper/best_flights_2008/part-r-00000'
catalog = sc.textFile(data_catalog).cache()
best_am = get_best_flight(catalog, 'SLC', 'BFL', '2008-01-04', 'AM')
```
Saving to Cassandra
```
data_catalog = 'hdfs://localhost:9000/user/sniper/best_flights_2008/part-r-00000'
catalog = sc.textFile(data_catalog).cache()
rdd = catalog.map(lambda line: line.split())
rdd2 = rdd.map(lambda tuple: (tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5], tuple[6], float(tuple[7])))
from pyspark.sql import Row
rdd3 = rdd2.map(lambda row: Row(airport_from=row[0], airport_to=row[1], given_date=row[2], am_or_pm=row[3], carrier=row[4], flight_num=row[5], departure_time=row[6], arr_delay=row[7])).cache()
df = spark.createDataFrame(rdd3)
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="best_flights_2008", keyspace="aviation")\
    .save()
```