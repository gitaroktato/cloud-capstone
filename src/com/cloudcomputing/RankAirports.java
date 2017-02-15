package com.cloudcomputing;


import com.cotdp.hadoop.ZipFileInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Created by sniper on 2017.02.13..
 */
public class RankAirports {

    private static final Log LOG = LogFactory.getLog(RankAirports.class);



    public static class MyMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text line, Context context)
                throws IOException, InterruptedException {
            String[] results = line.toString().split("\\s");
            context.write(new IntWritable(
                    Integer.parseInt(results[1])), new Text(results[0]));
        }
    }

    public static class MyReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Standard stuff
        Job job = Job.getInstance(conf, RankAirports.class.getName());
        job.setJarByClass(RankAirports.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

// The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
        job.waitForCompletion(true);
    }
}
