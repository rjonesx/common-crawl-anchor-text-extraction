package com.moz.commoncrawl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregate the anchor counts across multiple segments. Specify
 * -Danchors.threshold to limit the outputs
 **/
public class AnchorAggregator extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory
            .getLogger(AnchorAggregator.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AnchorAggregator(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // s3://anchorcc/anchors/*
        String inputPath = args[0];

        // s3://anchorcc/anchors-aggregation
        String outputPath = args[1];

        Job job = Job.getInstance(getConf(), "AnchorAggregator");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setReducerClass(AnchorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
