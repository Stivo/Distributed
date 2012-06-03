#cd ../; sbt dsl/scalariform-format dsl/test:scalariform-format ; cd -
cd ../; cd scoobi; sbt package-hadoop; cp target/*.jar ../benchmarking/progs; cd ../benchmarking
cd ../; cd crunch; sbt assembly; cp target/*.jar ../benchmarking/progs/crunch-gen.jar; cd ../benchmarking
#cd ../; sbt spark/package; cp spark/target/scala-2.9.2/*.jar benchmarking/progs; cd -
cd ../; cp spark/target/scala-2.9.2/*.jar benchmarking/progs; cd -
