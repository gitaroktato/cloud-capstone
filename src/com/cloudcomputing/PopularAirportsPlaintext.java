package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * This uses airline_ontime data to sum origin-destination airports
 */
public class PopularAirportsPlaintext {


    public static class Mapper
            extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DESTINATION_AIRPORT)
                    .forEach(tokens -> {
                        // FROM
                        try {
                            word.set(tokens[ORIGIN_AIRPORT].replaceAll("\"", ""));
                            context.write(word, one);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                        // TO
                        try {
                            word.set(tokens[DESTINATION_AIRPORT].replaceAll("\"", ""));
                            context.write(word, one);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }

    public static class Reducer
            extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, PopularAirportsPlaintext.class.getName());
        job.setJarByClass(PopularAirportsPlaintext.class);
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
