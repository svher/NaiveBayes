package com.svher;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class TrainMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(TrainMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        LOG.info(String.valueOf(split.getPath()));
        context.write(value, new IntWritable(1));
    }
}
