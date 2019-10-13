package com.svher;

import com.svher.wordcount.CalcProb;
import com.svher.wordcount.Verify;
import com.svher.wordcount.WordCount;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;


public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("local")) {
            deleteRecursive(Paths.get("Outputs"));
        }

        ToolRunner.run(new WordCount(), args);
        ToolRunner.run(new CalcProb(), args);
        ToolRunner.run(new Verify(), args);
    }

    static void deleteRecursive(Path path) throws IOException {
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