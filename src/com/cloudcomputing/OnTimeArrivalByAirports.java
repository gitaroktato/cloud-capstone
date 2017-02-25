package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.stream.Stream;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;

/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class OnTimeArrivalByAirports {

    public static class MyMapper
            extends Mapper<Object, Text, TupleTextWritable, DoubleWritable> {
        private TupleTextWritable airportAndCarrier = new TupleTextWritable();
        private DoubleWritable departureDelay = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DEPARTURE_DELAY)
                    .forEach(tokens -> {
                        try {
                            String originAirportValue = tokens[ORIGIN_AIRPORT].replaceAll("\"", "");
                            String destAirportValue = tokens[DESTINATION_AIRPORT].replaceAll("\"", "");
                            String delayValue = tokens[ARRIVAL_DELAY].replaceAll("\"", "");
                            // Skip empty values
                            if (delayValue.isEmpty()) {
                                return;
                            }
                            airportAndCarrier.setFirstKey(originAirportValue);
                            airportAndCarrier.setSecondKey(destAirportValue);

                            departureDelay.set(Double.parseDouble(delayValue));
                            context.write(airportAndCarrier, departureDelay);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }
    
    public static class MyReducer
            extends Reducer<TupleTextWritable, DoubleWritable, TupleTextWritable, Text> {
        public void reduce(TupleTextWritable key, Iterable<DoubleWritable> values, Context context)
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
        Job job = Job.getInstance(conf, OnTimeArrivalByAirports.class.getName());
        job.setJarByClass(OnTimeArrivalByAirports.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(TupleTextWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
