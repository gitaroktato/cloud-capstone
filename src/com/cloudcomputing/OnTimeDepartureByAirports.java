package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class OnTimeDepartureByAirports {

    public static class AirportCarrierWritable implements WritableComparable<AirportCarrierWritable> {

        private String airport;
        private String carrier;

        public AirportCarrierWritable() {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            airport = in.readUTF();
            carrier = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(airport);
            out.writeUTF(carrier);
        }

        @Override
        public int compareTo(AirportCarrierWritable other) {
            int airportComparison = airport.compareTo(other.airport);
            if (airportComparison != 0) {
                return airportComparison;
            }
            return carrier.compareTo(other.carrier);
        }

        @Override
        public String toString() {
            return airport + ' ' + carrier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AirportCarrierWritable that = (AirportCarrierWritable) o;

            if (airport != null ? !airport.equals(that.airport) : that.airport != null) return false;
            return carrier != null ? carrier.equals(that.carrier) : that.carrier == null;
        }

        @Override
        public int hashCode() {
            int result = airport != null ? airport.hashCode() : 0;
            result = 31 * result + (carrier != null ? carrier.hashCode() : 0);
            return result;
        }
    }

    public static class MyMapper
            extends Mapper<Object, Text, AirportCarrierWritable, DoubleWritable> {
        private AirportCarrierWritable airportAndCarrier = new AirportCarrierWritable();
        private DoubleWritable departureDelay = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DEPARTURE_DELAY)
                    .forEach(tokens -> {
                        try {
                            String carrierValue = tokens[CARRIER_ID].replaceAll("\"", "");
                            String airportValue = tokens[ORIGIN_AIRPORT].replaceAll("\"", "");
                            String delayValue = tokens[DEPARTURE_DELAY].replaceAll("\"", "");
                            // Skip empty values
                            if (delayValue.isEmpty()) {
                                return;
                            }
                            airportAndCarrier.airport = airportValue;
                            airportAndCarrier.carrier = carrierValue;

                            departureDelay.set(Double.parseDouble(delayValue));
                            context.write(airportAndCarrier, departureDelay);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }

    /**
     * Reducer for the ZipFile test, identical to the standard WordCount example
     */
    public static class MyReducer
            extends Reducer<AirportCarrierWritable, DoubleWritable, AirportCarrierWritable, Text> {
        public void reduce(AirportCarrierWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new Text(String.format("%.2f", average)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Standard stuff
        Job job = Job.getInstance(conf, OnTimeDepartureByAirports.class.getName());
        job.setJarByClass(OnTimeDepartureByAirports.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(AirportCarrierWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
