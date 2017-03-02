# common-crawl-anchor-text-extraction

This repository contains some code and resources to extract anchor text and URLs from WAT files out of the [CommonCrawl](http://commoncrawl.org/) datasets.

The anchors for A href elements are extracted and counted for links pointing to external domains from the one of the source page. The link extraction is similar but instead of extracting the anchors, we output the URL of the source document if it contains more than 5 links to external domains.

This code serves as an illustration on how to process the [WAT format](http://commoncrawl.org/the-data/get-started/#WAT-Format) with MapReduce in Java. It should be easy to modify for other purposes.

The instructions below are based on linux scripts. You will need a Hadoop cluster (e.g. AWS EMR) to run the map reduce code.

## generate the paths for the CC segments
- install s3cmd
- run path_gen.sh

This will produce a file s3.segments which will be used for the map reduce jobs. Each data release has exactly 100 segments.
The s3.segments given here correspond to the January 2017 dataset (CC-MAIN-2017-04).

## installation

The script _extractionLoop.sh_ must be put on the master node of your Hadoop cluster. If you are using EMR, just ssh to the master node. The script contains references to a S3 bucket where the jar file and list of segments can be taken from. Either compile the project with maven and push the files to the master node or store them on a S3 bucket and use the script to retrieve them for you.

Note: if your Hadoop cluster is not on EMR, you'll need to set the S3 credentials in the script to be able to access the CommonCrawl dataset.

## running the extraction

From the master node run 

`nohup ./extractionLoop.sh > anchor.log &`

this will download the list of segments and jar file from S3. You can remove the corresponding lines from the script if you put these files directly to the server.

The script takes a single optional argument `link` which determines whether to run the extraction of the links or anchor text. 

The output of the jobs is put in a S3 bucket but this can be replaced with a HDFS path.

## aggregating the results

The script mentioned above generates one output per CC segment. There are two classes to aggregate the results into a single path, e.g.

`hadoop jar anchor-extractor-1.0.jar com.moz.commoncrawl.AnchorAggregator -D anchors.threshold=5 s3n://anchorcc/anchors/* s3n://anchorcc/2017-anchors 10`

where the 3rd argument is the number of reducers to use (i.e. how many subfiles will be produced).

The command for aggregating the link files is

`hadoop jar anchor-extractor-1.0.jar com.moz.commoncrawl.LinkAggregator s3n://anchorcc/links-* s3n://anchorcc/2017-links 10`


