package com.geek.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowBeanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // get job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // define jar
        job.setJarByClass(FlowBeanDriver.class);
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // define input output
        FileInputFormat.setInputPaths(job, new Path("saliu/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("saliu/output"));
        // submit the job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}