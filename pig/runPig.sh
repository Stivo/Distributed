export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64

rm output -rf

export PIG_CLASSPATH=/home/stivo/master/distributed/pig/udfs.jar
TIMEARGS="\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
INPUT=/home/stivo/master/testdata/wiki2009-articles-10k.tsv
PROG=Wordcount2009.pig

/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input=$INPUT Wordcount2009.pig

INPUT=/home/stivo/master/testdata/tpch/small/
#INPUT=/home/stivo/master/testdata/tpch/smaller/

#/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input=$INPUT -p shipmodes="TRUCK|SHIP" -p startdate=1995-01-01 -p enddate=1996-01-01 -p output="output" Q12.pig

#head output/part-*
