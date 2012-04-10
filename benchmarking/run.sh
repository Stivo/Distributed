#!/bin/bash
#PROG=examples.weblog.WebLogAnalyzer
#PROG=generated.test.WordCountJob
#PROG=examples.join.JoinExampleJob
#PROG=examples.wikilog.WikipediaLog
PROG=scoobi.generated.PageCountApp
PROG=scoobi.generated.TpchQueries
#PROG=scoobi.generated.WordCountApp

for VERSION in $PROG #${PROG}_Orig
do
echo Running $VERSION
if [[ $2 ]]; then
du -h $2
rm -rf $2
fi;
set -x
time env HADOOP_HEAPSIZE="4096" hadoop jar progs/Scoobi*.jar $VERSION $@ 2> ./$VERSION.txt
if [[ $2 ]]; then
du -h $2
rm -rf $2
fi;

done
