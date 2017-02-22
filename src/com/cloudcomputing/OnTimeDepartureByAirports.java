package com.cloudcomputing;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import static com.cloudcomputing.OntimePerformanceMetadata.*;
/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class OnTimeDepartureByAirports {

    public static class AirportCarrierWritable implements WritableComparable<AirportCarrierWritable> {

        private String airport;
        private String carrier;

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

    }

    public static class MyMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text carrierId = new Text();
        private DoubleWritable arrivalDelay = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= ARRIVAL_DELAY)
                    .forEach(tokens -> {
                        try {
                            String carrierValue = tokens[CARRIER_ID].replaceAll("\"", "");
                            String delayValue = tokens[ARRIVAL_DELAY].replaceAll("\"", "");
                            // Skip empty values
                            if (delayValue.isEmpty()) {
                                return;
                            }
                            carrierId.set(carrierValue);
                            arrivalDelay.set(Double.parseDouble(delayValue));
                            context.write(carrierId, arrivalDelay);
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
            extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
