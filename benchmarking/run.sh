#!/bin/bash
#PROG=examples.weblog.WebLogAnalyzer
#PROG=generated.test.WordCountJob
#PROG=examples.join.JoinExampleJob
#PROG=examples.wikilog.WikipediaLog
#PROG=scoobi.generated.PageCountApp
#PROG=scoobi.generated.TpchQueries
PROG=scoobi.generated.WordCountApp
#HADOOP_HOME=/home/stivo/hadooptemp/hadoop-0.20.2-cdh3u3/
HADOOPHOME=$HADOOP_HOME/bin/
HADOOPHOME=""
A=""
for x in {0..5}
do
A="$A$x\n"
#A="$A${x}w\n"
done

for version in $(echo -e $A | sort )
do

PROG=scoobi.generated.v$version.WordCountApp
PROG=scoobi.generated.v$version.TpchQueries
OUTPUT=output_v$version

rm -rf $OUTPUT
INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
INPUTS="/home/stivo/master/testdata/tpch/small/ $OUTPUT 1995-01-01 TRUCK SHIP"
cat /home/stivo/master/testdata/tpch/small/orders.tbl /home/stivo/master/testdata/tpch/small/lineitem.tbl > /dev/null
TIMEARGS="s$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/scoobi-gen*.jar $PROG $INPUTS 2> ./scoobi_$PROG.txt
cat /home/stivo/master/testdata/tpch/small/orders.tbl /home/stivo/master/testdata/tpch/small/lineitem.tbl > /dev/null

#du -h $OUTPUT
TIMEARGS="c$version\t%e\t%S\t%U\t%M\t%P"
PROG=crunch.generated.v$version.TpchQueries
/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/crunch-gen.jar $PROG "asdf" $INPUTS 2> ./crunch_$PROG.txt 
done

