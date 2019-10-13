package com.svher.wordcount;

import com.svher.util.TextDoublePair;
import com.svher.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Verify extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Verify.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(VerifyMapper.class);
        job.setReducerClass(VerifyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextDoublePair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("Outputs/probs/seq*"));
        FileOutputFormat.setOutputPath(job, new Path("Outputs/final"));
        return job.waitForCompletion(false) ? 1 : 0;
    }

    static class VerifyMapper extends Mapper<TextPair, DoubleWritable, Text, TextDoublePair> {
        @Override
        protected void map(TextPair key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key.getFirst(), new TextDoublePair(key.getSecond(), value));
        }
    }

    static class VerifyReducer extends Reducer<Text, TextDoublePair, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TextDoublePair> values, Context context) throws IOException, InterruptedException {
            double max = Double.NEGATIVE_INFINITY;
            Text ans = new Text();
            for (TextDoublePair value : values) {
                if (value.getSecond().get() > max) {
                    max = value.getSecond().get();
                    ans.set(value.getFirst());
                }
            }
            context.write(key, ans);
        }
    }


}
