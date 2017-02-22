package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RankAirports {

    public static class MyMapper
            extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text line, Context context)
                throws IOException, InterruptedException {
            String[] results = line.toString().split("\\s");
            context.write(new LongWritable(
                    Long.parseLong(results[results.length - 1])), new Text(results[0]));
        }
    }

    public static class MyReducer
            extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
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
        //
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

// The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
        job.waitForCompletion(true);
    }
}
