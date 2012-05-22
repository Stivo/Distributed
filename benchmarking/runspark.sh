#set -x
CLASSPATH=:../spark/lib/spark-core-assembly-0.4-SNAPSHOT.jar:progs/spark_2.9.1-0.1-SNAPSHOT.jar
PROG=spark.examples.PageCountApp
PROG=spark.examples.TpchQueries
PROG=spark.examples.WordCountApp
#PROG=spark.examples.KMeansApp
#PROG=spark.examples.SparkKMeans
PROG_BAK=$PROG
JAVA=/usr/lib/jvm/java-1.6.0-openjdk/bin/java
HEAP=4g
CORES=1

rm -rf output/
./warmup.sh

#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG mesos://master@localhost:5050 $@ 2> ./spark_$PROG.txt
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt > ./spark_$PROG.txt
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
