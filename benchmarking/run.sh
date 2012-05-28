#!/bin/bash
#PROG=examples.weblog.WebLogAnalyzer
#PROG=generated.test.WordCountJob
#PROG=examples.join.JoinExampleJob
#PROG=examples.wikilog.WikipediaLog
PROG=scoobi.generated.PageCountApp
#PROG=scoobi.generated.TpchQueries
#PROG=scoobi.generated.WordCountApp
#HADOOP_HOME=/home/stivo/hadooptemp/hadoop-0.20.2-cdh3u3/
HADOOPHOME=$HADOOP_HOME/bin/
HADOOPHOME=""

for version in {0..3}
do

#PROG=spark.examples.v$version.WordCountApp
PROG=scoobi.generated.v$version.TpchQueries
OUTPUT=output_v$version

rm -rf $OUTPUT

INPUTS="/home/stivo/master/testdata/tpch/small/ $OUTPUT 1995-01-01 TRUCK SHIP"
TIMEARGS="$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/Scoobi*.jar $PROG $INPUTS 2> ./scoobi_$PROG.txt 
#du -h $OUTPUT

done

