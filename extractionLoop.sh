#!/bin/bash

# iterates on the list of segments
# the argument indicates whether to process the anchors or the links
# if using outside of AWS you'll need to set the credentials for the hadoop operations to S3
# e.g. -D fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY -D fs.s3n.awsSecretAccessKey=$AWS_SECRET_KEY 

modeval="anchor"

if [ "$1" = "link" ]; then
   modeval="link"
fi

# take the jar from S3
hadoop fs -copyToLocal s3n://anchorcc/anchor-extractor-1.0-SNAPSHOT.jar ~/anchor-extractor-1.0-SNAPSHOT.jar

# take the list of segments from S3 unless there already is one from S3
hadoop fs -copyToLocal s3n://anchorcc/unprocessed.$modeval.segments ~/unprocessed.$modeval.segments

if [ ! -f ~/unprocessed.$modeval.segments ]; then
 hadoop fs -copyToLocal s3n://anchorcc/s3.segments ~/unprocessed.$modeval.segments
fi

lastSegment=`tail -n1 ~/unprocessed.$modeval.segments`

while [ "$lastSegment" != "" ]; do 

  if [ -e ".STOP" ]
  then
   echo "STOP file found - escaping loop"
   break
  fi

	echo "Processing $lastSegment for $modeval"

	if [ "$modeval" = "anchor" ]; 
     then 
       hadoop jar ~/anchor-extractor-1.0-SNAPSHOT.jar com.moz.commoncrawl.WATAnchorExtractor "$lastSegment" s3n://anchorcc/anchors/
     else 
       hadoop jar ~/anchor-extractor-1.0-SNAPSHOT.jar com.moz.commoncrawl.WATLinkExtractor "$lastSegment" s3n://anchorcc/links/
	fi

	RETVAL=$?
	if [ $RETVAL -ne 0 ] 
	 then 
		echo "Segment $lastSegment exited with code $RETVAL"
        echo "$lastSegment" >> ~/failed.$modeval.segments
     else
        echo "Segment $lastSegment processed successfully"
	fi

	head -n -1 ~/unprocessed.$modeval.segments > temp.txt
	mv temp.txt ~/unprocessed.$modeval.segments

	# delete the existing version
	hadoop fs -rmr s3n://anchorcc/unprocessed.$modeval.segments 

	# send it back to the bucket
	hadoop fs -copyFromLocal ~/unprocessed.$modeval.segments s3n://anchorcc/unprocessed.$modeval.segments 

	lastSegment=`tail -n1 ~/unprocessed.$modeval.segments`

done

