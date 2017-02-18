package com.cloudcomputing;


import com.cotdp.hadoop.ZipFileInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by sniper on 2017.02.13..
 */
public class PopularAirportsPlaintext {

    private static final Log LOG = LogFactory.getLog(PopularAirportsPlaintext.class);


    /**
     * This Mapper class checks the filename ends with the .txt extension, cleans
     * the text and then applies the simple WordCount algorithm.
     */
    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public static final int ORIGIN_AT = 6;
        public static final int DESTINATION_AT = 14;
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Pattern.compile("\n", Pattern.MULTILINE)
                    .splitAsStream(value.toString())
                    .map(line -> line.split(","))
                    .filter( tokens -> tokens.length >= DESTINATION_AT)
                    .forEach(tokens -> {
                        // FROM
                        word.set(tokens[ORIGIN_AT]);
                        try {
                            context.write(word, one);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        // TO
                        word.set(tokens[DESTINATION_AT]);
                        try {
                            context.write(word, one);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    });
        }
    }

    /**
     * Reducer for the ZipFile test, identical to the standard WordCount example
     */
    public static class MyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
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
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
