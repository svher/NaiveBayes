package com.svher;

import com.svher.job.WordCount;
import com.svher.util.Prediction;
import com.svher.util.Utils;
import com.svher.job.Classify;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("local")) {
            Utils.deleteRecursive("Outputs");
        }

        ToolRunner.run(new WordCount(), args);
        ToolRunner.run(new Classify(), args);
    }
}