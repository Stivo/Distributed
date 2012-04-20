#!/bin/bash
#PROG=examples.weblog.WebLogAnalyzer
#PROG=generated.test.WordCountJob
#PROG=examples.join.JoinExampleJob
#PROG=examples.wikilog.WikipediaLog
PROG=scoobi.generated.PageCountApp
PROG=scoobi.generated.TpchQueries
#PROG=scoobi.generated.WordCountApp
HADOOP_HOME=/home/stivo/hadooptemp/hadoop-0.20.2-cdh3u3/
HADOOPHOME=$HADOOP_HOME/bin/
HADOOPHOME=""
for VERSION in $PROG ${PROG}_Orig

do
echo Running $VERSION
if [[ $2 ]]; then
du -h $2
rm -rf $2
fi;
set -x
time env HADOOP_HEAPSIZE="4096" ${HADOOPHOME}hadoop jar progs/Scoobi*.jar $VERSION $@ 2> ./$VERSION.txt
#time env HADOOP_HEAPSIZE="4096" $HADOOPHOMEhadoop jar progs/Scoobi*.jar $VERSION $@$VERSION 2> ./$VERSION.txt
if [[ $2 ]]; then
du -h $2
rm -rf $2
fi;

done
