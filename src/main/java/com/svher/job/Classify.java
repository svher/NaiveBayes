package com.svher.job;

import com.svher.format.withpath.CombineTextInputFormatWithFileName;
import com.svher.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Classify extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Classify.class);

    static class ClassifyMapper extends Mapper<Text, Text, Text, DoubleArrayWritable> {
        Prediction pred;
        @Override
        protected void setup(Context context) throws IOException {
            pred = new Prediction(context.getConfiguration());
            pred.initialize(true);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, new DoubleArrayWritable(pred.getPredictionVector(value)));
        }
    }

    static class ClassifyReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
        Prediction pred;
        @Override
        protected void setup(Context context) throws IOException {
            pred = new Prediction(context.getConfiguration());
            pred.initialize(false);
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
            double[] probs = null;
            for (DoubleArrayWritable value : values) {
                Writable[] arr = value.get();
                if (probs == null) probs = new double[arr.length];
                for (int i = 0; i < arr.length; i++) {
                    probs[i] += ((DoubleWritable)arr[i]).get();
                }
            }
            context.write(key, pred.makePrediction(probs));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setInputFormatClass(CombineTextInputFormatWithFileName.class);
        job.setMapperClass(ClassifyMapper.class);
        job.setReducerClass(ClassifyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("etc/Test/*/*"));
        FileOutputFormat.setOutputPath(job, new Path("Outputs/classify"));

        int completionResult = job.waitForCompletion(false) ? 1 : 0;
        calcMetrics();
        return completionResult;
    }

    private void calcMetrics() throws IOException {
        Map<String, Integer> mapTP = new HashMap<>();
        Map<String, Integer> mapTN = new HashMap<>();
        Map<String, Integer> mapFN = new HashMap<>();
        Set<String> classNames = new HashSet<>();

        FileSystem fs = FileSystem.get(URI.create(""), getConf());
        FileStatus[] statuses = fs.globStatus(new Path("Outputs/classify/part-r*"));
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] s0 = line.split("\t");
                    String[] s1 = s0[0].split("/");
                    String trueClass = s1[0];
                    String predClass = s0[1];
                    if (trueClass.equals(predClass)) mapTP.put(trueClass, mapTP.getOrDefault(trueClass, 0) + 1);
                    else {
                        mapFN.put(trueClass, mapFN.getOrDefault(trueClass, 0) + 1);
                        mapTN.put(predClass, mapTN.getOrDefault(trueClass, 0) + 1);
                    }
                    classNames.add(trueClass);
                    classNames.add(predClass);
                }
            }
        }
        // 计算宏平均
        double macroPrecision = 0, macroRecall = 0, macroF1 = 0;
        int totalTP = 0, totalFN = 0, totalTN = 0;
        for (String className : classNames) {
            double tp = mapTP.getOrDefault(className, 0);
            double tn = mapTN.getOrDefault(className, 0);
            double fn = mapFN.getOrDefault(className, 0);
            double precision = tp / (tp + tn);
            if (Double.isNaN(precision)) precision = 1;
            double recall = tp / (tp + fn);
            if (Double.isNaN(recall)) recall = 1;
            double f1 = 2 * precision * recall / (precision + recall);
            if (Double.isNaN(f1)) {
                if (precision == 0 && recall == 0) f1 = 0;
                else f1 = 1;
            }
            macroPrecision += precision;
            macroRecall += recall;
            macroF1 += f1;
            totalTP += tp;
            totalTN += tn;
            totalFN += fn;
            LOG.info(String.format("Class:%s Precision:%.2f Recall:%.2f F1:%.2f", className, precision, recall, f1));
        }
        macroPrecision /= classNames.size();
        macroRecall /= classNames.size();
        macroF1 /= classNames.size();
        LOG.info(String.format("MacroAveraged - Precision:%.2f Recall:%.2f F1:%.2f", macroPrecision, macroRecall, macroF1));
        // 计算微平均
        double microPrecision = (double)totalTP / (totalTP + totalTN);
        double microRecall = (double)totalTP / (totalTP + totalFN);
        double microF1 = 2 * microPrecision * microRecall / (microPrecision + microRecall);
        LOG.info(String.format("MicroAveraged - Precision:%.2f Recall:%.2f F1:%.2f", microPrecision, microRecall, microF1));
    }
}
