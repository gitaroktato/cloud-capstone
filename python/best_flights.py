"""best_flights.py"""
import sys
from pyspark import SparkContext

def get_best_flight(catalog, from_airport, to_airport, date, am_or_pm):
    line = catalog.filter(lambda line: from_airport in line
                                       and to_airport in line
                                       and date in line
                                       and am_or_pm in line).first()
    tuple = line.split()
    return tuple[4:]

from_airport = sys.argv[1]
to_airport = sys.argv[2]
given_date = sys.argv[3]
# Searching for
print("Looking for %s %s %s" % (from_airport, to_airport, given_date))

# Should be some file on your system
data_catalog = 'hdfs://localhost:9000/user/sniper/best_flights_2008/part-r-00000'
sc = SparkContext("local", "Best Flights")
catalog = sc.textFile(data_catalog).cache()

best_am = get_best_flight(catalog, from_airport, to_airport, given_date, 'AM')

print("Best in the morning %s %s %s %s" % best_am)

sc.stop()