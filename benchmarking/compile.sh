cd ../; sbt dsl/scalariform-format dsl/test:scalariform-format; cd -
cd ../scoobi; sbt package-hadoop; cp target/*.jar ../benchmarking/progs; cd -
cd ../; sbt spark/package; cp spark/target/scala-2.9.1/*.jar benchmarking/progs; cd -
