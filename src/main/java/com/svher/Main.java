package com.svher;

import com.svher.wordcount.CalcProb;
import com.svher.wordcount.CalcTotalWords;
import com.svher.wordcount.Verify;
import com.svher.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("local")) {
            deleteRecursive(Paths.get("Outputs/probs"));
            deleteRecursive(Paths.get("Outputs/final"));
            deleteRecursive(Paths.get("Outputs/total"));
        }

        //ToolRunner.run(new CalcTotalWords(), args);
        //ToolRunner.run(new WordCount(), args);
        ToolRunner.run(new CalcProb(), args);
        ToolRunner.run(new Verify(), args);
    }

    public static void deleteRecursive(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}