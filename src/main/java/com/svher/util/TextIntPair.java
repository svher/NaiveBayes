package com.svher.util;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextIntPair implements WritableComparable<TextIntPair> {
    private Text first;
    private IntWritable second;

    public Text getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public TextIntPair() {
        set(new Text(), new IntWritable());
    }

    public TextIntPair(Text first, IntWritable second) {
        set(first, second);
    }

    public void set(Text first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextIntPair o) {
        int cmp = first.compareTo(o.first);
        return cmp != 0 ? cmp : second.compareTo(o.second);
    }

    @Override
    public String toString() {
        return first.toString() + " " + second.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
}
