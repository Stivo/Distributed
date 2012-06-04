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
A="$A${x}w\n"
A="$A${x}k\n"
done

for version in $(echo -e $A | sort )
do
	for BACKEND in crunch
	do
		PROG=$BACKEND.generated.v$version.TpchQueries
		#PROG=$BACKEND.generated.v$version.WordCountApp
		DESC=${BACKEND:0:1}_v$version
		OUTPUT=output_$DESC
		rm -rf $OUTPUT
		TPCHDATA="/home/stivo/master/testdata/tpch/small/"
		INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
		INPUTS="$TPCHDATA $OUTPUT 1995-01-01 TRUCK SHIP"
		cat $TPCHDATA/orders.tbl $TPCHDATA/lineitem.tbl > /dev/null
		TIMEARGS="${BACKEND}_${version}\t%e\t%S\t%U\t%M\t%P"
		#echo "$BACKEND $DESC $OUTPUT $PROG"
		#-XX:MaxInlineSize=1000	
		/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/${BACKEND}-gen*.jar $PROG $INPUTS 2> ./${PROG}_${DESC}.txt 
		
	done
done

