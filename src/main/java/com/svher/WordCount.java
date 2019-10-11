package com.svher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCount extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        getConf().set("mapreduce.output.textoutputformat.separator", ":");
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setMapperClass(TrainMapper.class);
        job.setReducerClass(TrainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(false) ? 1 : 0;
    }
}
