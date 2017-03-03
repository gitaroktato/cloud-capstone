# Data Cleaning And exploration
## Aviation Dataset Mount & Initialization
To be able to work with data, first we have to mount the transportation dataset to our EC2 instance.
I used the `lsblk` command to see available block storages mounted to my EC2. Then created a separate folder and mounted the volume to the filesystem.
```bash
$ lsblk
$ sudo mkdir /data
$ mount /dev/xvdb /data
```
### References
* [AWS User Guide: Attaching EBS volumes](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-attaching-volume.html)
* [AWS User Guide: Using EBS volumes](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)

## Data exploration using `bash`
At a certain level bash is perfectly fine to discover what's included in the transportation dataset. At first with directory navigation we can investigate each folder and see what's inside.
Obviously we're just interested in the `aviation` subfolder.

```bash
$ ls
air_carrier_employees          air_carrier_statistics_summary  airline_origin_destination    aviation_safety_reporting
...
$ cd airline_ontime
$ ls
1988  1989  1990  1991  1992  1993  1994  1995  1996  1997  1998
...
$ cd 2008
$ ls
On_Time_On_Time_Performance_2008_10.zip  On_Time_On_Time_Performance_2008_2.zip  On_Time_On_Time_Performance_2008_6.zip
...
```
We can peek inside each zip file using `bash`as well, ff we pipe the output of each file to the `gunzip` command. Directing the output to an other file allows us to save samples and test our map-reduce jobs on small portion of data.
```bash
cat ./On_Time_On_Time_Performance_2008_10.zip | gunzip | head -255 > ~/airline_ontime_perf.csv
```
## Moving relevant data to Hadoop HDFS
We'll just work with the `airline_ontime` data, which contains on-time performance for each flight. A special `bash` script is getting all the zip archives in all subfolders, searches inside each zip file for CSV extensions and unzips only those files from the zip archive. We'll pipe each CSV output to a `hdfs` `put` command.
```bash
migration/move-ontime-perf-to-hadoop.sh /data/aviation /user/ec2-user/ontime_perf
```
### References
* [Migration scripts on GitHub](https://github.com/gitaroktato/cloud-capstone/blob/master/migration/move-ontime-perf-to-hadoop.sh)

# System Integration
All heavyweight data simplifying and transformation jobs are done by map-reduce over YARN. These jobs most of the time crawl all the CSV files under `airline_ontime` subfolder. The goal of these map-reduce jobs is to prepare dataset digestible for Spark. Spark provides the final results over this "clean" dataset.

![System Overview](system-overview.png)
## Cassandra Migration
Migrating to Apache Cassandra is done by using Spark Cassandra Connector. This allows shifting loaded DataFrames from Spark to Apache Cassandra.

### References
* [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)
* [PySpark with Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/15_python.md)

# Solution Approach
## Question 1.1
_Rank the top 10 most popular airports by numbers of flights to/from the airport._

A map-reduce job collects all the from-to airport field from `airline_ontime` data and counts each airport. This is very similar to the well-known Word Count example in Hadoop documentation.

<..., line> -> **map()** -> <airport_id, 1> -> **reduce()** -> <airport_id, occurrence>

Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.PopularAirportsPlaintext ontime_perf popular_airports
```

As result we get one file with airports in alphabetical order.
```
ABE	236094
ABI	39323
ABQ	1428081
...
```


This file is sorted and trimmed using `PySpark`
```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/popular_airports/part-r-00000')
rdd = file.cache()
rdd.map(lambda line: line.split()).filter(lambda tuple: len(tuple) == 2).filter(lambda tuple: len(tuple[0]) == 3).map(lambda tuple: (int(tuple[1]), tuple[0])).sortByKey(ascending=False).take(10)
```
### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/PopularAirportsPlaintext.java)


## Question 1.2
_Rank the top 10 airlines by on-time arrival performance._

The solution is similar to the previous. We get each carrier and it's arrival delay field. Then at the reduce phase we calculate the average arrival delay for each carrier.

<..., line> -> **map()** -> <carrier_id, arrival_delay> -> **reduce()** -> <carrier_id, average_arrival_delay>

Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.AverageDelays ontime_perf avg_delays
```

As result we get one file with carriers in alphabetical order.
```
9E	5.87
AA	7.11
AL	8.29
...
```

This file is then sorted and trimmed using `PySpark`
```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/avg_delays/part-r-00000')
rdd = file.cache()
rdd = rdd.map(lambda line: line.split()).cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[1]), tuple[0])).cache()
rdd2.takeOrdered(10)
```

### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/AverageDelays.java)

## Question 2.1
_For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X._

For this calculation, the map-reduce jobs will use a custom compound key, that's composed from airport ID and carrier ID.
Map jobs will emit each **<airport,carrier>** key pair's on-time departure performance and reduce jobs will calculate average for each **<airport,carrier>** key pair.

<..., line> -> **map()** -> <(airport_id, carrier_id), departure_delay> -> **reduce()** -> <(airport_id, carrier_id), average_departure_delay>

Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.OnTimeDepartureByCarriers ontime_perf departure_by_carriers
```

As result we get one file with airports and carriers in alphabetical order. Each value represents the on-time departure average for that airport-carrier pair.

```
ABE 9E	6.75
ABE AA	4.76
ABE AL	3.63
ABE DH	5.34
ABE DL	23.28
...
```

This file is then sorted using `PySpark`
```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/departure_by_carriers/part-r-00000')
rdd = file.map(lambda line: line.split()).cache()
rdd = rdd.filter(lambda tuple: tuple[0] == 'CMI').cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0])).cache()
rdd2.takeOrdered(10)
```

Saving to Cassandra is done by using the spark-cassandra-connector package. We have to mark this dependency, when starting up PySpark

```bash
./bin/pyspark --conf spark.cassandra.connection.host=<CASSANDRA_HOST> --packages datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11
```

PySpark command, that loads results and then moves to Cassandra node.

```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/departure_by_carriers/part-r-00000')
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

Cassandra table definition is the following

```sql
create keyspace aviation WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
create table aviation.airport_carrier_departure (
  airport text,
  carrier text,
  dep_delay decimal,
  PRIMARY KEY(airport, dep_delay, carrier)
);
```

Using the table to determine top performer can be done with this CQL command

```
select * from aviation.airport_carrier_departure where airport = 'MIA' order by dep_delay limit 3;

 airport | dep_delay | carrier
---------+-----------+---------
     MIA |      -3.0 |      9E
     MIA |       1.2 |      EV
     MIA |       1.3 |      RU

(3 rows)
```

### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/OnTimeDepartureByCarriers.java)

## Question 2.2
_For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X._

This is very similar to solution to Question 2.1. Here the compound key consists origin and destination airport.

<..., line> -> **map()** -> <(airport_from, airport_to), departure_delay> -> **reduce()** -> <(airport_from, airport_to), average_departure_delay>

Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.OnTimeDepartureByAirports ontime_perf departure_by_airports
```

Sample from map-reduce result

```
ABE ALB	10.00
ABE ATL	10.03
ABE AVP	3.44
ABE AZO	241.00
ABE BDL	0.00
...
```

This file is then sorted using `PySpark`
```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/departure_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split()).cache()
rdd = rdd.filter(lambda tuple: tuple[0] == 'CMI').cache()
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0])).cache()
rdd2.takeOrdered(10)
```

PySpark command, that loads results and then moves to Cassandra node.

```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/departure_by_airports/part-r-00000')
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

Cassandra table definition is the following

```sql
create table aviation.airport_airport_departure (
  airport text,
  airport_to text,
  dep_delay decimal,
  PRIMARY KEY(airport, dep_delay, airport_to)
);
```

CQL query example

```sql
select * from aviation.airport_airport_departure where airport = 'LAX' order by dep_delay limit 3;

 airport | dep_delay | airport_to
---------+-----------+------------
     LAX |     -16.0 |        SDF
     LAX |      -7.0 |        IDA
     LAX |      -6.0 |        DRO

(3 rows)
```

### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/OnTimeDepartureByAirports.java)


## Question 2.4

_For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y._

This is very similar to solution to Question 2.2. Here the compound value consists arrival delay instead of departure delay.

<..., line> -> **map()** -> <(airport_from, airport_to), arrival_delay> -> **reduce()** -> <(airport_from, airport_to), average_arrival_delay>


Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.OnTimeArrivalByAirports ontime_perf arrival_by_airports
```

Sample from map-reduce result

```
ABE ALB	23.00
ABE ATL	7.65
ABE AVP	2.35
ABE BDL	1.00
ABE BHM	-3.00
...
```

This file is then sorted using `PySpark`
```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/arrival_by_airports/part-r-00000')
rdd = file.map(lambda line: line.split())
rdd2 = rdd.filter(lambda tuple: tuple[0] == 'CMI' and tuple[1] == 'ORD')
rdd2 = rdd.map(lambda tuple: (float(tuple[2]), tuple[1], tuple[0]))
rdd2.collect()
```

PySpark command, that loads results and then moves to Cassandra node.

```
file = sc.textFile('hdfs://localhost:9000/user/ec2-user/arrival_by_airports/part-r-00000')
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


Cassandra table definition is the following

```sql
create table aviation.airport_airport_arrival (
  airport text,
  airport_to text,
  arr_delay decimal,
  PRIMARY KEY(airport, airport_to)
);
```

CQL query example

```sql
cqlsh> select * from aviation.airport_airport_arrival where airport = 'DFW' and airport_to = 'IAH';

 airport | airport_to | arr_delay
---------+------------+-----------
     DFW |        IAH |      7.62

(1 rows)
```

### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/OnTimeArrivalByAirports.java)


## Question 3.2

_Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way._

We use the same approach except, that key-value pairs will be both custom Writable extensions in this case.
Keys will be constructed from (airport_from, airport_to, flight_date, am_or_pm). Values will be created from (carrier_id, flight_num, departure_time, arrival_delay). Reduce jobs will get all arrival delays from all the keys.

The main goal here is to be able to tell average delay for all the origin-destination pair at a given date. Distinguishing morning and afternoon flights. Because the problem can be split into two independent events (getting from X to Y and from Y to Z), we can answer each questions by two queries.

<..., line> -> **map()** -> <(airport_from, airport_to, flight_date, am_or_pm), (carrier_id, flight_num, departure_time, arrival_delay)> -> **reduce()** -> <(airport_from, airport_to, flight_date, am_or_pm), (carrier_id, flight_num, departure_time, average_arrival_delay)>

Map-Reduce execution is done by the following command.

```bash
bin/hadoop jar ~/IdeaProjects/cloud-capstone/out/artifacts/cloud_capstone/cloud-capstone.jar com.cloudcomputing.BestFlightOnAGivenDate ontime_perf/*2008*.csv best_flights_2008
```
Sample from map-reduce result

```
ABE ATL 2008-01-01 PM	EV 4833 12:05 12.00
ABE ATL 2008-01-02 AM	EV 4923 06:00 -19.00
ABE ATL 2008-01-02 PM	EV 4206 17:17 -35.00
ABE ATL 2008-01-03 AM	EV 4877 06:40 409.00
ABE ATL 2008-01-03 PM	EV 4206 17:20 13.00
...
```

Searching for a given flight can be done by `PySpark` using two different queries. One from searching at travel date with AM and another one with two days after using PM as parameter.

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
get_best_flight(catalog, 'SLC', 'BFL', '2008-01-04', 'AM')
get_best_flight(catalog, 'BFL', 'JFK', '2008-01-06', 'PM')
```


PySpark command, that loads results and then moves to Cassandra node.

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

Cassandra table definition is the following

```sql
create table aviation.best_flights_2008 (
  airport_from text,
  airport_to text,
  given_date date,
  am_or_pm text,
  carrier text,
  flight_num text,
  departure_time text,
  arr_delay decimal,
  PRIMARY KEY(airport_from, airport_to, given_date, am_or_pm)
);
```

CQL query example

```sql
cqlsh> select * from aviation.best_flights_2008 where airport_from = 'SLC' and airport_to = 'BFL' and given_date = '2008-01-06' and am_or_pm = 'AM';

 airport_from | airport_to | given_date | am_or_pm | arr_delay | carrier | departure_time | flight_num
--------------+------------+------------+----------+-----------+---------+----------------+------------
          SLC |        BFL | 2008-01-06 |       AM |     100.0 |      OO |          11:40 |       3755

(1 rows)
```

### References
[Map-Reduce job](https://github.com/gitaroktato/cloud-capstone/blob/master/src/com/cloudcomputing/BestFlightOnAGivenDate.java)
