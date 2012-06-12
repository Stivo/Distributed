export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64

rm output -rf

RUN=18
for x in {1..3}
do
RUN=$[RUN+1]
export PIG_CLASSPATH=/etc/hadoop/conf.whirr/
export HADOOP_CONF_DIR=/etc/hadoop/conf.whirr/
TIMEARGS="pig\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
PROG=Wordcount2009.pig

TPCHDATA=/home/stivo/master/testdata/tpch/small/
#INPUT=/home/stivo/master/testdata/tpch/smaller/
HDFS="hdfs://ec2-23-20-53-52.compute-1.amazonaws.com/user/stivo/"
TPCHDATA="$HDFS/inputs/tpch/s100/ "
INPUT=$TPCHDATA
OUTPUT="$HDFS/outputs/pig/tpch/s100/job$RUN/"

#INPUT=/home/stivo/master/testdata/wiki2009-articles-1k.tsv
#INPUT="$HDFS/inputs/wiki/s2/"
#OUTPUT="$HDFS/outputs/wiki/s2/pig/run$RUN/"
#/usr/bin/time -f $TIMEARGS -o /dev/stdout /home/stivo/master/pig-0.10.0/bin/pig -p input=$INPUT -p output="$OUTPUT" Wordcount2009.pig
#/usr/bin/time -f $TIMEARGS -o /dev/stdout java -Xmx256m -cp udfs.jar:udfs/pig-0.10.0.jar org.apache.pig.Main -p input=$INPUT -p output="output_pig_wc" -x local  Wordcount2009.pig


/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input=$INPUT -p shipmodes="TRUCK|SHIP" -p startdate=1995-01-01 -p enddate=1996-01-01 -p output="$OUTPUT" Q12.pig
#/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input=$INPUT -p shipmodes="TRUCK|SHIP" -p startdate=1995-01-01 -p enddate=1996-01-01 -p output="$OUTPUT" Q12Mapper.pig
done
