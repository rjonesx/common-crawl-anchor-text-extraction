rm -rf paths
mkdir paths

s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-07/warc.paths.gz  paths/2016-07.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-18/warc.paths.gz  paths/2016-18.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-22/warc.paths.gz  paths/2016-22.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-26/warc.paths.gz  paths/2016-26.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-30/warc.paths.gz  paths/2016-30.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-36/warc.paths.gz  paths/2016-36.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-40/warc.paths.gz  paths/2016-40.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-44/warc.paths.gz  paths/2016-44.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2016-50/warc.paths.gz  paths/2016-50.warc.paths.gz
s3cmd get s3://commoncrawl/crawl-data/CC-MAIN-2017-04/warc.paths.gz  paths/2017-04.warc.paths.gz

cd paths

zcat 201*.paths.gz | sed 's/warc/\t/' | cut -f1 | sort | uniq > ../segments

# 100 segments per release -> 1K in total

# prepend the path 

cd ..

while read p; do echo "s3://commoncrawl/$p"; done < segments > s3.segments

rm -rf segments

