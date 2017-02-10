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
