package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.stream.Stream;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;

/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class AllFlightsWithDepartureAndArrivalDelay {

    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= ARRIVAL_DELAY)
                    .forEach(tokens -> {
                        try {
                            String originAirportValue = tokens[ORIGIN_AIRPORT].replaceAll("\"", "");
                            String destAirportValue = tokens[DESTINATION_AIRPORT].replaceAll("\"", "");
                            String departureDate = tokens[DEPARTURE_DATE].replaceAll("\"", "");
                            String carrierId = tokens[CARRIER_ID].replaceAll("\"", "");
                            String flightNum = tokens[FLIGHT_NUM].replaceAll("\"", "");
                            String departureTime = tokens[SCHEDULED_DEPARTURE_TIME].replaceAll("\"", "");
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
                            context.write(new Text(originAirportValue),
                                    new Text(String.join(" ", destAirportValue, carrierId,
                                            flightNum, departureDate, departureTimePretty, arrivalDelayValue)));
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }

    public static class MyReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Standard stuff
        Job job = Job.getInstance(conf, AllFlightsWithDepartureAndArrivalDelay.class.getName());
        job.setJarByClass(AllFlightsWithDepartureAndArrivalDelay.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
