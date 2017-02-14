package com.cloudcomputing;


import com.cotdp.hadoop.ZipFileInputFormat;
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
import java.util.StringTokenizer;

/**
 * Created by sniper on 2017.02.13..
 */
public class PopularAirports {

    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Standard stuff
        Job job = Job.getInstance(conf, PopularAirports.class.getName());
        job.setJarByClass(PopularAirports.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

// Hello there ZipFileInputFormat!
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

// The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

// We want to be fault-tolerant
        ZipFileInputFormat.setLenient( true );
        ZipFileInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
//
        job.waitForCompletion(true);
    }
}
