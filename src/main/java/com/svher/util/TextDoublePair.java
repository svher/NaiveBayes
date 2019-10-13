package com.svher.util;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextDoublePair implements WritableComparable<TextDoublePair> {
    private Text first;
    private DoubleWritable second;

    public Text getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

    public TextDoublePair() {
        set(new Text(), new DoubleWritable());
    }

    public TextDoublePair(Text first, DoubleWritable second) {
        set(first, second);
    }

    public void set(Text first, DoubleWritable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextDoublePair o) {
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
