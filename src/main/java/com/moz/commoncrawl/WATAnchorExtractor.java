package com.moz.commoncrawl;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCInputFormat;

/**
 * Processes a WAT segment from CC, parses the JSON for the metadata and
 * processes the outlinks.
 */
public class WATAnchorExtractor extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(WATAnchorExtractor.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WATAnchorExtractor(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// s3://commoncrawl/crawl-data/CC-MAIN-2016-07/segments/1454701146196.88/
		String segmentPath = args[0];
		// s3://anchorcc/
		String outputPath = args[1];

		// get the ref of the segment e.g. 14547...
		Pattern segmPattern = Pattern.compile("segments/(.+)/");
		Matcher match = segmPattern.matcher(segmentPath);
		match.find();
		String segmID = match.group(1);

		outputPath += segmID;

		segmentPath += "/wat/*.wat.gz";

		Job job = Job.getInstance(getConf(), "WATAnchorExtractor");
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(WARCInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(segmentPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		// FileOutputFormat.setCompressOutput(job, true);
		job.setMapperClass(WATParserMapper.class);
		job.setReducerClass(AnchorReducer.class);
		job.setCombinerClass(AnchorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AnchorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

	public static class WATParserMapper extends Mapper<LongWritable, WARCWritable, Text, IntWritable> {

		private static ObjectMapper mapper = new ObjectMapper();

		private static final IntWritable ONE = new IntWritable(1);

		@Override
		protected void map(LongWritable key, WARCWritable value, Context context)
				throws IOException, InterruptedException {

			WARCRecord record = value.getRecord();

			String recordType = record.getHeader().getRecordType();
			if (!recordType.equals("metadata")) {
				return;
			}

			// parse the json content
			JsonNode jsonNode = mapper.readValue(record.getContent(), JsonNode.class);

			JsonNode wtu = jsonNode.get("Envelope").get("WARC-Header-Metadata").get("WARC-Target-URI");
			if (wtu == null)
				return;

			String sourceURL = wtu.getTextValue();
			if (StringUtils.isBlank(sourceURL)) {
				return;
			}

			if (!"response".equalsIgnoreCase(
					jsonNode.get("Envelope").get("WARC-Header-Metadata").get("WARC-Type").getTextValue())) {
				return;
			}

			try {
				// check the outlinks
				Iterator<JsonNode> links = jsonNode.get("Envelope").get("Payload-Metadata")
						.get("HTTP-Response-Metadata").get("HTML-Metadata").get("Links").iterator();

				while (links.hasNext()) {
					JsonNode link = links.next();
					if (!"A@/href".equals(link.get("path").getTextValue()))
						continue;
					String anchorText = link.get("text").getTextValue();
					anchorText = anchorText.toLowerCase();
					anchorText = anchorText.trim();
					anchorText = anchorText.replaceAll("\\s+", " ");
					context.write(new Text(anchorText), ONE);
				}
			} catch (Exception e) {
				String errorMessage = "Exception while parsing " + sourceURL + ": " + e;
				LOG.error(errorMessage, e);
				return;
			}
		}

	}

}
