package com.svher.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class Prediction {
    private static final Logger LOG = LoggerFactory.getLogger(Prediction.class);
    private Configuration conf;
    private int totalClass;
    private String[] classNames;
    private double[] priorProbs;
    private Map<String, Integer> classIdxs;
    private Map<String, DoubleWritable[]> conditionalProbs;

    public Prediction(Configuration conf) {
        this.conf = conf;
    }

    public void initialize(boolean loadConditional) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(""), conf);
        String line;
        List<Integer> numDocuments = new ArrayList<>();
        double totalDocuments = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("etc/params"))))) {
            List<String> lstNames = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                String[] splits = line.split(" ");
                lstNames.add(splits[0]);
                int numDocument = Integer.parseInt(splits[1]);
                numDocuments.add(numDocument);
                totalDocuments += numDocument;
            }
            classNames = lstNames.toArray(new String[0]);
            totalClass = classNames.length;
        }
        classIdxs = new HashMap<>();
        priorProbs = new double[totalClass];
        for (int i = 0; i < totalClass; i++) {
            classIdxs.put(classNames[i], i);
            priorProbs[i] = Math.log(numDocuments.get(i) / totalDocuments);
        }
        if (loadConditional) loadConditionalProb();
    }

    private void loadConditionalProb() throws IOException {
        conditionalProbs = new HashMap<>();
        int[] numWords = new int[classNames.length];
        Map<String, int[]> docWords = new HashMap<>();
        Set<String> terms = new HashSet<>();

        FileSystem fs = FileSystem.get(URI.create(""), conf);
        FileStatus[] statuses = fs.globStatus(new Path("Outputs/wordcount/part-r*"));
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] s0 = line.split("\t");
                    String[] s1 = s0[0].split("#");
                    int idx = classIdxs.get(s1[0]);
                    int wordCount = Integer.parseInt(s0[1]);
                    String word = s1[1];
                    if (!docWords.containsKey(word)) docWords.put(word, new int[totalClass]);
                    docWords.get(word)[idx] = wordCount;
                    numWords[idx] += wordCount;
                    terms.add(word);
                }
            }
        }
        // 这里放入一个 ##UNK## 单词表示没有在训练集中出现的单词对应的概率，使用 Laplace 平滑计算得到
        docWords.put("##UNK##", new int[totalClass]);
        int totalTerms = terms.size();
        conditionalProbs = new HashMap<>();
        for (String word : docWords.keySet()) {
            int[] counts = docWords.get(word);
            DoubleWritable[] wordProbs = new DoubleWritable[totalClass];
            for (int i = 0; i < totalClass; i++) {
                double prob = Math.log((counts[i] + 1.) / ((numWords[i] + totalTerms)));
                wordProbs[i] = new DoubleWritable(prob);
                conditionalProbs.put(word, wordProbs);
            }
        }
    }

    public DoubleWritable[] getPredictionVector(Text word) {
        return conditionalProbs.getOrDefault(word.toString(), conditionalProbs.get("##UNK##"));
    }

    public Text makePrediction(double[] input) {
        double max = Double.NEGATIVE_INFINITY;
        int argmax = 0;
        for (int i = 0; i < totalClass; i++) {
            double withPrior = input[i] + priorProbs[i];
            if (withPrior > max) {
                max = withPrior;
                argmax = i;
            }
        }
        return new Text(classNames[argmax]);
    }
}
