package com.svher.wordcount;

import com.sun.org.apache.xpath.internal.operations.Mult;
import com.svher.Main;
import com.svher.util.TextIntPair;
import com.svher.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalcTotalWords extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(CalcProb.class);

    static class WordsMapper extends Mapper<TextPair, IntWritable, Text, NullWritable> {
        @Override
        protected void map(TextPair key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key.getSecond(), NullWritable.get());
        }
    }

    static class WordsReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.getCounter("count", "total").increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(WordsMapper.class);
        job.setReducerClass(WordsReducer.class);
        job.setCombinerClass(WordsReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("Outputs/wordcount/*"));
        FileOutputFormat.setOutputPath(job, new Path("Outputs/total"));
        int ret = job.waitForCompletion(false) ? 1 : 0;
        LOG.info("total words: {}", job.getCounters().getGroup("count").findCounter("total").getValue());
        return ret;
    }
}
