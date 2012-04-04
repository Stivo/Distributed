#!/bin/bash
#set -x
#PROG=examples.weblog.WebLogAnalyzer
#PROG=generated.test.WordCountJob
#PROG=examples.join.JoinExampleJob
#PROG=examples.wikilog.WikipediaLog
PROG=scoobi.generated.PageCountApp
PROG=scoobi.generated.TpchQueries

for VERSION in $PROG #${PROG}_Orig
do
echo Running $VERSION
#if [[ $2 ]]; then
#rm -rf $2
#fi;
time hadoop jar progs/Scoobi*.jar $VERSION $@ 2> ./$VERSION.txt
#if [[ $2 ]]; then
#du -h $2
#rm -rf $2
#fi;

done
