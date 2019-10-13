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

public class CalcProb extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(CalcProb.class);
    private static int numTerms = 5000;

    static class ProbMapper extends Mapper<TextPair, IntWritable, Text, TextIntPair> {
        @Override
        protected void map(TextPair key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key.getFirst(), new TextIntPair(key.getSecond(), value));
        }
    }

    static class ProbReducer extends Reducer<Text, TextIntPair, TextPair, DoubleWritable> {
        private static String testSuite = "etc/Test/*/*";
        FileSystem fs;
        MultipleOutputs<TextPair, DoubleWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
            fs = FileSystem.get(URI.create(testSuite), context.getConfiguration());

            /* 还是不加先验概率，因为有部分类别数量极少
            Map<String, Double> priorProb = new HashMap<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("etc/params"))))) {
                String line = reader.readLine();
                int total = Integer.parseInt(line);
                line = reader.readLine();
                while (line != null) {
                    String[] splits = line.split(" ");
                    priorProb.put(splits[0], Math.log(Integer.parseInt(splits[1])/(double)total));
                    line = reader.readLine();
                }
            } */
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<TextIntPair> values, Context context) throws IOException, InterruptedException {
            int numTokens = 0;
            List<TextIntPair> pairs = new ArrayList<>();
            Map<String, Double> weights = new HashMap<>();
            for (TextIntPair pair : values) {
                numTokens += pair.getSecond().get();
                weights.put(pair.getFirst().toString(), (double) pair.getSecond().get());
            }
            for (String wkey : weights.keySet()) {
                weights.put(wkey, Math.log((weights.get(wkey)+1d)/(numTokens+numTerms)));
            }
            FileStatus[] statuses = fs.globStatus(new Path(testSuite));
            for (FileStatus status : statuses) {
                double prob = 0;
                Path path = status.getPath();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line = reader.readLine();
                    while(line != null) {
                        prob += weights.getOrDefault(line, Math.log(1d/numTerms));
                        line = reader.readLine();
                    }
                    multipleOutputs.write(new TextPair(new Text(path.getParent().getName()+"#"+path.getName()), key), new DoubleWritable(prob), "seq");
                    // multipleOutputs.write("plain", key, new DoubleWritable(prob), path.getParent().getName()+"#"+path.getName());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ProbMapper.class);
        job.setReducerClass(ProbReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextIntPair.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleOutputs.addNamedOutput(job,"plain", TextOutputFormat.class, Text.class, DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("Outputs/wordcount/*"));
        FileOutputFormat.setOutputPath(job, new Path("Outputs/probs"));
        return job.waitForCompletion(false) ? 1 : 0;
    }
}
