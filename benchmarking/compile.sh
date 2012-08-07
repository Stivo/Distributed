cd ../; sbt dsl/scalariform-format dsl/test:scalariform-format crunch/assembly spark/assembly ; cd -
cd /home/stivo/master/comparisons/manual; sbt assembly; cd -
cp /home/stivo/master/comparisons/manual/target/hadoop*.jar progs/manual-gen-assembly.jar

#cd ../; sbt crunch/assembly ; cd -
cp ../scoobi/target/*.jar progs
#cp ../crunch/target/*.jar progs
cp ../spark/target/spark-gen-assembly*.jar progs/spark-gen.jar
#cd ../; cd scoobi; sbt package-hadoop; cp target/*.jar ../benchmarking/progs; cd ../benchmarking
#cd ../; cd crunch; sbt assembly; cp target/*.jar ../benchmarking/progs/crunch-gen.jar; cd ../benchmarking
#cd ../; sbt spark/assembly; cp spark/target/spark-gen-assembly*.jar benchmarking/progs/spark-gen.jar; cd -
#cd ../; cp spark/target/scala-2.9.2/*.jar benchmarking/progs; cd -
