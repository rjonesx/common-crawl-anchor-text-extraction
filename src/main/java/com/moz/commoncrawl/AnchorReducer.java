package com.moz.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnchorReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int threshold = -1;

    @Override
    protected void setup(
            Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        threshold = context.getConfiguration().getInt("anchors.threshold", -1);
    }

    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : counts) {
            sum += count.get();
        }
        if (threshold == -1 || sum > threshold)
            context.write(word, new IntWritable(sum));
    }
}