package com.cloudcomputing;


import com.cotdp.hadoop.ZipFileInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This uses airline_ontime data to sum origin-destination airports
 */
public class PopularAirportsPlaintext {

    private static final Log LOG = LogFactory.getLog(PopularAirportsPlaintext.class);


    /**
     * This Mapper class checks the filename ends with the .txt extension, cleans
     * the text and then applies the simple WordCount algorithm.
     */
    public static class MyMapper
            extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        public static final int ORIGIN_AT = 11;
        public static final int DESTINATION_AT = 18;
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DESTINATION_AT)
                    .forEach(tokens -> {
                        // FROM
                        word.set(tokens[ORIGIN_AT]);
                        try {
                            context.write(word, one);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                        // TO
                        word.set(tokens[DESTINATION_AT]);
                        try {
                            context.write(word, one);
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
            extends Reducer<Text, LongWritable, Text, LongWritable> {
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
        // Standard stuff
        Job job = Job.getInstance(conf, PopularAirportsPlaintext.class.getName());
        job.setJarByClass(PopularAirportsPlaintext.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
