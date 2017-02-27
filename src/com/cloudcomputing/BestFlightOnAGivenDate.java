package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Stream;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;


public class BestFlightOnAGivenDate {

    private static final String MORNING = "AM";
    private static final String AFTERNOON = "PM";

    public static class CompoundKeyWritable
            implements WritableComparable<CompoundKeyWritable> {

        private String airportFrom;
        private String airportTo;
        private String flightDate;
        private String morningOrAfternoon;

        @Override
        public void readFields(DataInput in) throws IOException {
            airportFrom = in.readUTF();
            airportTo = in.readUTF();
            flightDate = in.readUTF();
            morningOrAfternoon = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(airportFrom);
            out.writeUTF(airportTo);
            out.writeUTF(flightDate);
            out.writeUTF(morningOrAfternoon);
        }


        @Override
        public int compareTo(CompoundKeyWritable other) {
            int comparison;
            comparison = airportFrom.compareTo(other.airportFrom);
            if (comparison != 0) {
                return comparison;
            }
            comparison = airportTo.compareTo(other.airportTo);
            if (comparison != 0) {
                return comparison;
            }
            comparison = flightDate.compareTo(other.flightDate);
            if (comparison != 0) {
                return comparison;
            }
            return morningOrAfternoon.compareTo(other.morningOrAfternoon);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompoundKeyWritable that = (CompoundKeyWritable) o;

            if (!airportFrom.equals(that.airportFrom)) return false;
            if (!airportTo.equals(that.airportTo)) return false;
            if (!flightDate.equals(that.flightDate)) return false;
            return morningOrAfternoon.equals(that.morningOrAfternoon);
        }

        @Override
        public int hashCode() {
            int result = airportFrom.hashCode();
            result = 31 * result + airportTo.hashCode();
            result = 31 * result + flightDate.hashCode();
            result = 31 * result + morningOrAfternoon.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return String.join(" ", airportFrom, airportTo, flightDate, morningOrAfternoon);
        }
    }

    public static class CompoundValueWritable
            implements WritableComparable<CompoundValueWritable>, Cloneable {

        private String carrierId;
        private String flightNum;
        private String departureTime;
        private String arrivalDelay;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(carrierId);
            out.writeUTF(flightNum);
            out.writeUTF(departureTime);
            out.writeUTF(arrivalDelay);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            carrierId = in.readUTF();
            flightNum = in.readUTF();
            departureTime = in.readUTF();
            arrivalDelay = in.readUTF();
        }

        @Override
        public int compareTo(CompoundValueWritable other) {
            int comparison;
            comparison = carrierId.compareTo(other.carrierId);
            if (comparison != 0) {
                return comparison;
            }
            comparison = flightNum.compareTo(other.flightNum);
            if (comparison != 0) {
                return comparison;
            }
            comparison = departureTime.compareTo(other.departureTime);
            if (comparison != 0) {
                return comparison;
            }
            return arrivalDelay.compareTo(other.arrivalDelay);
        }

        @Override
        public String toString() {
            return String.join(" ", carrierId, flightNum, departureTime, arrivalDelay);
        }

        @Override
        public Object clone() {
            try {
                return super.clone();
            } catch (CloneNotSupportedException e) {
                // Should not reach here
                return null;
            }
        }
    }

    public static class MyMapper
            extends Mapper<Object, Text, CompoundKeyWritable, CompoundValueWritable> {

        private CompoundKeyWritable key = new CompoundKeyWritable();
        private CompoundValueWritable value = new CompoundValueWritable();

        public void map(Object dummy, Text line, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(line.toString())
                    .map(l -> l.split(","))
                    .filter(tokens -> tokens.length >= ARRIVAL_DELAY)
                    .forEach(tokens -> {
                        try {
                            String originAirportValue = tokens[ORIGIN_AIRPORT].replaceAll("\"", "");
                            String destAirportValue = tokens[DESTINATION_AIRPORT].replaceAll("\"", "");
                            String departureDate = tokens[DEPARTURE_DATE].replaceAll("\"", "");
                            String carrierId = tokens[CARRIER_ID].replaceAll("\"", "");
                            String flightNum = tokens[FLIGHT_NUM].replaceAll("\"", "");
                            String departureTime = tokens[SCHEDULED_DEPARTURE_TIME].replaceAll("\"", "");
                            String morningOrAfternoon = morningOrAfternoon(departureTime);
                            String departureTimePretty = departureTime.substring(0, 2)
                                    + ":" + departureTime.substring(2, 4);
                            String arrivalDelayValue = tokens[ARRIVAL_DELAY].replaceAll("\"", "");
                            // Skip garbage
                            if (originAirportValue.length() != 3) {
                                return;
                            }
                            // Skip empty values
                            if (arrivalDelayValue.isEmpty()) {
                                return;
                            }
                            key.airportFrom = originAirportValue;
                            key.airportTo = destAirportValue;
                            key.flightDate = departureDate;
                            key.morningOrAfternoon = morningOrAfternoon;
                            value.carrierId = carrierId;
                            value.flightNum = flightNum;
                            value.departureTime = departureTimePretty;
                            value.arrivalDelay = arrivalDelayValue;
                            context.write(key, value);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }

        private String morningOrAfternoon(String departureTime) {
            int depTimeHours = Integer.parseInt(departureTime.substring(0,2));
            if (depTimeHours < 12) {
                return MORNING;
            } else {
                return AFTERNOON;
            }
        }
    }

    public static class MyReducer
            extends Reducer<CompoundKeyWritable, CompoundValueWritable,
            CompoundKeyWritable, CompoundValueWritable> {

        @Override
        public void reduce(CompoundKeyWritable key,
                           Iterable<CompoundValueWritable> values,
                           Context context)
                throws InterruptedException, IOException {
            // Get the best arrival time
            CompoundValueWritable minimum = null;
            for (CompoundValueWritable value : values) {
                if (minimum == null) {
                    // 
                    minimum = (CompoundValueWritable) value.clone();
                } else {
                    minimum = (CompoundValueWritable) getValueWithMinimumArrivalTime(
                            minimum, value).clone();
                }
            }
            context.write(key, minimum);
        }

        private CompoundValueWritable getValueWithMinimumArrivalTime(CompoundValueWritable minimum,
                                                                     CompoundValueWritable value) {
            double minimumArrivalTime;
            double newArrivalTime;
            // Safe cast for min
            try {
                minimumArrivalTime = Double.parseDouble(minimum.arrivalDelay);
            } catch (NumberFormatException ex) {
                return value;
            }
            // Safe cast for new
            try {
                newArrivalTime = Double.parseDouble(value.arrivalDelay);
            } catch (NumberFormatException ex) {
                return minimum;
            }
            if (newArrivalTime < minimumArrivalTime) {
                return value;
            } else {
                return minimum;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Standard stuff
        Job job = Job.getInstance(conf, BestFlightOnAGivenDate.class.getName());
        job.setJarByClass(BestFlightOnAGivenDate.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(CompoundKeyWritable.class);
        job.setOutputValueClass(CompoundValueWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
