#!/bin/bash

touch allresults.txt

OUTPUT=output
HDFS="hdfs://ec2-23-22-2-78.compute-1.amazonaws.com/user/stivo/"
A=""


for x in {0..4}
do
A="$A$x\n"
#A="$A${x}w\n"
#A="$A${x}k\n"
done
REDUCERS=40
for RUN in {1..3} #{10..12}
do
for BACKEND in crunch #scoobi #scrunch crunch
do
	for version in $(echo -e $A )
	do	
		JOB=$(python jobnumber.py)
		
		OUTPUT="$HDFS/outputs/job$JOB/v$version"
		#if [ "$BACKEND" = "crunch" ]; then
		#	version="${version}k"
		#fi
		DESC=${BACKEND:0:1}_job$JOB_v$version

		#OUTPUT="output_$DESC"
		#rm -rf $OUTPUT
		TIMEARGS="${BACKEND}\t${version}\t%e\t$JOB\t%S\t%U\t%M\t%P"

		#### TPCH ####
		PROG=dcdsl.generated.v$version.TpchQueries
		#TPCHDATA="/home/stivo/master/testdata/tpch/gb3/"
		TPCHDATA="$HDFS/inputs/tpch/s200/"
		#PROG=dcdsl.generated.v$version.TpchQueriesMapper
		#OUTPUT="$HDFS/outputs/tpch/run$JOB/output_$DESC/"
		INPUTS="$TPCHDATA $OUTPUT 1995-01-01 TRUCK|SHIP $REDUCERS"
		#cat $TPCHDATA/lineitem.tbl > /dev/null
		#cat $TPCHDATA/orders.tbl > /dev/null

		#### WIKI ####
		PROG=dcdsl.generated.v$version.WordCountApp
		#OUTPUT="$HDFS/outputs/wiki/s2/run$JOB/output_$DESC"
		INPUTS="$HDFS/inputs/wiki/ $OUTPUT $REDUCERS"
		#INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
		#echo "$BACKEND $DESC $OUTPUT $PROG"
		#-XX:MaxInlineSize=1000	
		#echo $TIMEARGS
		#/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/${BACKEND}-gen*.jar $PROG $INPUTS 2> ./${PROG}_${DESC}.txt
		/usr/bin/time -f $TIMEARGS -o timedata env HADOOP_HEAPSIZE="4096" hadoop jar progs/${BACKEND}-gen*.jar $PROG $INPUTS 2> ./${PROG}_${DESC}_$JOB.txt
		cat timedata
		cat timedata >> allresults.txt
				
	done
done
done

for RUN in {1..3}  #{10..12}
do
for BACKEND in manual #scoobi #scrunch crunch
do
	for version in 4
	do	
		JOB=$(python jobnumber.py)
		
		OUTPUT="$HDFS/outputs/job$JOB/"
		#if [ "$BACKEND" = "crunch" ]; then
		#	version="${version}k"
		#fi
		DESC=${BACKEND:0:1}_job$JOB_v$version

		#OUTPUT="output_$DESC"
		#rm -rf $OUTPUT
		TIMEARGS="${BACKEND}\t${version}\t%e\t$JOB\t%S\t%U\t%M\t%P"

		#### TPCH ####
		PROG=dcdsl.generated.v$version.TpchQueries
		#TPCHDATA="/home/stivo/master/testdata/tpch/gb3/"
		TPCHDATA="$HDFS/inputs/tpch/s200/"
		#PROG=dcdsl.generated.v$version.TpchQueriesMapper
		#OUTPUT="$HDFS/outputs/tpch/run$JOB/output_$DESC/"
		INPUTS="$TPCHDATA $OUTPUT 1995-01-01 TRUCK|SHIP $REDUCERS"
		#cat $TPCHDATA/lineitem.tbl > /dev/null
		#cat $TPCHDATA/orders.tbl > /dev/null

		#### WIKI ####
		PROG=dcdsl.generated.v$version.WordCountApp
		#OUTPUT="$HDFS/outputs/wiki/s2/run$JOB/output_$DESC"
		INPUTS="$HDFS/inputs/wiki/ $OUTPUT $REDUCERS"
		#INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
		#echo "$BACKEND $DESC $OUTPUT $PROG"
		#-XX:MaxInlineSize=1000	
		#echo $TIMEARGS
		/usr/bin/time -f $TIMEARGS -o timedata env HADOOP_HEAPSIZE="4096" hadoop jar progs/${BACKEND}-gen*.jar $PROG $INPUTS 2> ./${PROG}_${DESC}_$JOB.txt
		cat timedata
		cat timedata >> allresults.txt
		done
	done
done
