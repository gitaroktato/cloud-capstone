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
```

# Uploading data to HDFS
```
sudo -u hdfs hadoop fs -mkdir /user/hive/warehouse/transportation
sudo -u hdfs hadoop fs -copyFromLocal ~/transportation/*.zip /user/hive/warehouse/transportation
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
