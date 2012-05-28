export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64

rm output -rf

export PIG_CLASSPATH=/home/stivo/master/distributed/pig/piggybank.jar
pig -p input=/home/stivo/Downloads/wex/start.tsv.head Wordcount.pig

head output/part-r-00000
