package com.svher.wordcount;

import com.svher.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCount extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WordMapper.class);

    static class WordMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private Text className = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            className.set(split.getPath().getName());
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("count", "total").increment(1);
            context.write(new TextPair(className, value), new IntWritable(1));
        }
    }

    static class WordReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable value : values) {
                cnt += value.get();
            }
            context.write(key, new IntWritable(cnt));
        }
    }

    static long numCount = 0;

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setCombinerClass(WordReducer.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("etc/Data/*"));
        FileOutputFormat.setOutputPath(job, new Path("Outputs/wordcount"));
        int ret = job.waitForCompletion(false) ? 1 : 0;
        numCount = job.getCounters().findCounter("count", "total").getValue();
        return ret;
    }
}
