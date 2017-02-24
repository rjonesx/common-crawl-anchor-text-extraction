package com.moz.commoncrawl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCInputFormat;

/**
 * Processes a WARC segment from CC, parses the HTML and extract the text of the
 * anchors. The output is a text file with the anchor text as key followed by a
 * marker indicating whether it is found in an A element with nofollow
 * attribute. A separate job will aggregate the outputs for the various CC
 * segments into 2 distinct files and dedup + count each entry.
 */
public class WARCAnchorExtractor extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory
            .getLogger(WARCAnchorExtractor.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WARCAnchorExtractor(), args);
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

        outputPath += segmID + ".counts";

        segmentPath += "/warc/*.warc.gz";

        Job job = Job.getInstance(getConf(), "WARCAnchorExtractor");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(WARCInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(segmentPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // FileOutputFormat.setCompressOutput(job, true);
        job.setMapperClass(WARCParserMapper.class);
        job.setReducerClass(AnchorReducer.class);
        job.setCombinerClass(AnchorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class WARCParserMapper
            extends Mapper<LongWritable, WARCWritable, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, WARCWritable value,
                Context context) throws IOException, InterruptedException {

            WARCRecord record = value.getRecord();

            String recordType = record.getHeader().getRecordType();
            String targetURL = record.getHeader().getTargetURI();

            if (!recordType.equals("response") || targetURL == null) {
                return;
            }

            HttpResponse response;
            try {
                response = new HttpResponse(record.getContent());
            } catch (Exception e) {
                LOG.error("Problem extracting response", e);
                return;
            }

            // check content type?
            String contentType = response.getHeader("Content-Type");
            if (contentType!=null && !contentType.contains("html")){
                return;
            }

            // TODO extract or compute charset?
            String charset = "UTF-8";

            // parse with JSOUP
            try (ByteArrayInputStream bais = new ByteArrayInputStream(
                    response.getContent())) {
                org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(bais, charset,
                        targetURL);
                Elements links = jsoupDoc.select("a[href]");
                for (Element link : links) {
                    // nofollow
                    boolean noFollow = "nofollow"
                            .equalsIgnoreCase(link.attr("rel"));
                    String anchor = link.text();
                    if (StringUtils.isNotBlank(anchor)) {
                        // lowercase, trim, remove extra whitespace
                        anchor = anchor.toLowerCase();
                        anchor = anchor.trim();
                        anchor = anchor.replaceAll("\\s+", " ");
                        // send to the output
                        context.write(new Text(anchor + "\t" + noFollow), ONE);
                    }
                }
            } catch (Throwable e) {
                String errorMessage = "Exception while parsing " + targetURL
                        + ": " + e;
                LOG.error(errorMessage, e);
                return;
            }
        }

    }

    public static class AnchorReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }
}
