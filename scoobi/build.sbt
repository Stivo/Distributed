name := "Scoobi Examples"

version := "0.1"

scalaVersion := "2.9.1"

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" // % "provided"

resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/"

resolvers += "Packaged Avro" at "http://nicta.github.com/scoobi/releases/"