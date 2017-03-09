package com.moz.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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

	private static final Logger LOG = LoggerFactory.getLogger(AnchorAggregator.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new AnchorAggregator(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// s3://anchorcc/anchors/*
		String inputPath = args[0];

		// s3://anchorcc/anchors-aggregation
		String outputPath = args[1];

		int numReducers = 1;
		if (args.length == 3) {
			numReducers = Integer.parseInt(args[2]);
		}

		LOG.info("Using {} reducers", numReducers);

		Job job = Job.getInstance(getConf(), "AnchorAggregator");
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);
		job.setMapperClass(AnchorMapper.class);
		job.setNumReduceTasks(numReducers);
		if (getConf().getBoolean("anchors.track.hosts", false)) {
			LOG.info("Using HostReducer");
			job.setReducerClass(HostReducer.class);
		} else {
			LOG.info("Using AnchorReducer");
			job.setReducerClass(AnchorReducer.class);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class AnchorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// separate the label from the value
			String s = value.toString();
			int tab = s.lastIndexOf('\t');
			int count = Integer.parseInt(s.substring(tab + 1));
			context.write(new Text(s.substring(0, tab)), new IntWritable(count));
		}
	}

	// intermediate step : counts an occurrence of host+anchor as 1
	static class HostReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			// separate the label from the value
			String s = word.toString();
			int tab = s.indexOf('\t');
			context.write(new Text(s.substring(tab + 1)), new IntWritable(1));
		}
	}

}
