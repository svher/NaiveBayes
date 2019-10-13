package com.svher.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    public static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator()
        {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int first = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int second = WritableUtils.decodeVIntSize(b2[s1]) + readVInt(b2, s1);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, first, b2, s2, first);
                return cmp != 0 ? cmp : TEXT_COMPARATOR.compare(b1, s1+first, l1, b2, s2+first, l2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }

        static {
            WritableComparator.define(TextPair.class, new Comparator());
        }
    }

    private Text first, second;

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "#" + second;
    }

    @Override
    public int compareTo(TextPair o) {
        int cmp = first.compareTo(o.first);
        return cmp != 0 ? cmp : second.compareTo(o.second);
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
