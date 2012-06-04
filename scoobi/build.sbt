name := "scoobi-gen"

version := "0.1"

scalaVersion := "2.9.2"

resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/"

resolvers += "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Packaged Avro" at "http://nicta.github.com/scoobi/releases/"

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" // % "provided"

libraryDependencies += "dk.brics.automaton" % "automaton" % "1.11-8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)
