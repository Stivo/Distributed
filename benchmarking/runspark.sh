#set -x
CLASSPATH=:../spark/lib/spark-core-assembly-0.4-SNAPSHOT.jar:progs/spark_2.9.1-0.1-SNAPSHOT.jar
PROG=spark.examples.PageCountApp
PROG=spark.examples.TpchQueries
PROG=spark.examples.WordCountApp
JAVA=/usr/lib/jvm/java-1.6.0-openjdk/bin/java
HEAP=1g
CORES=1
rm -rf output/
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt

du -h output/
rm -rf output/
PROG=${PROG}_Orig
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/
#time scala -cp $CLASSPATH ${PROG}_Orig local[1] $@
