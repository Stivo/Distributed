set -x
CLASSPATH=:../spark/lib/spark-core-assembly-0.4-SNAPSHOT.jar:progs/spark_2.9.1-0.1-SNAPSHOT.jar
PROG=spark.examples.PageCountApp

time scala -cp $CLASSPATH $PROG local[1] $@
time scala -cp $CLASSPATH ${PROG}_Orig local[1] $@
