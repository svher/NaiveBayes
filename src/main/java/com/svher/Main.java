package com.svher;

import com.svher.util.Utils;
import com.svher.wordcount.CalcProb;
import com.svher.wordcount.Verify;
import com.svher.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Verify.class);

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("local")) {
            Utils.deleteRecursive("Outputs");
//            Utils.deleteRecursive("Outputs/probs");
//            Utils.deleteRecursive("Outputs/final");
        }

        ToolRunner.run(new WordCount(), args);
        ToolRunner.run(new CalcProb(), args);
        ToolRunner.run(new Verify(), args);

        FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(""), new Configuration());
        FileStatus[] statuses = fs.globStatus(new Path("Outputs/final/part-r*"));
        Map<String, Integer> tp = new HashMap<>();
        Map<String, Integer> tn = new HashMap<>();
        Map<String, Integer> fn = new HashMap<>();
        for (FileStatus status : statuses) {
            double prob = 0;
            Path path = status.getPath();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line = reader.readLine();
                while (line != null) {
                    String rightClass = line.split("#")[0];
                    String classifiedClass = line.split("\t")[1];
                    if (rightClass.equals(classifiedClass)) tp.put(rightClass, tp.getOrDefault(rightClass, 0) + 1);
                    else {
                        tn.put(classifiedClass, tn.getOrDefault(classifiedClass, 0) + 1);
                        fn.put(rightClass, fn.getOrDefault(rightClass, 0) + 1);
                    }
                    line = reader.readLine();
                }
            }
        }
        double f1;
        int tp_val = 0, tn_val = 0, fn_val = 0;
        for (String classKey : tp.keySet()) {
            tp_val += tp.getOrDefault(classKey, 0);
            tn_val += tn.getOrDefault(classKey, 0);
            fn_val += fn.getOrDefault(classKey, 0);
        }
        double precision = tp_val+tn_val==0 ? 1 : (double)tp_val/(tp_val+tn_val);
        double recall = tp_val+fn_val==0 ? 1 : (double) tp_val/(tp_val+fn_val);
        f1 = (2*precision*recall)/(precision+recall);
        LOG.info("precision: {} recall: {}", precision, recall);
        LOG.info("f1: {}", f1);
    }
}