#set -x
CLASSPATH=:progs/spark-core-assembly.jar:progs/spark-gen.jar

JAVA=/usr/lib/jvm/java-1.6.0-openjdk/bin/java
HEAP=4g
CORES=1

#TIME=/usr/bin/time -f $TIMEARGS


#set -x

for version in {0..5}
do
#./warmup.sh
PROG=dcdsl.generated.v$version.WordCountApp
PROG=dcdsl.generated.v$version.TpchQueries
OUTPUT=output_v$version
INPUTS="/home/stivo/master/testdata/tpch/small/ $OUTPUT 1995-01-01 TRUCK SHIP"
#INPUTS="/home/stivo/Downloads/wex/start.tsv.head $OUTPUT"
#INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
#cat /home/stivo/Downloads/wex/start.tsv.head > /dev/null
#cat /home/stivo/master/testdata/tpch/small/linei*.tbl > /dev/null

TIMEARGS="$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $INPUTS 2> ./spark_$PROG.txt 
done

#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG mesos://master@localhost:5050 $@ 2> ./spark_$PROG.txt
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt 
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG_BAK local[$CORES] $@ 2> ./spark_$PROG_BAK.txt 
#PROG=spark.examples.SparkKMeans
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG_Orig.txt > ./spark_$PROG_Orig.txt


#du -h output/


set +x

A="""

rm -rf output/
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

rm -rf output/
PROG=${PROG}_Orig
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

PROG=$PROG_BAK
CORES=2
rm -rf output/
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

rm -rf output/
PROG=${PROG}_Orig
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/
"""
