import AssemblyKeys._ // put this at the top of the file

name := "crunch-gen"

version := "0.1.0"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Cloudera Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Cloudera Third-Party Releases" at "https://repository.cloudera.com/content/repositories/third-party/"
)

libraryDependencies ++= Seq(
  "com.cloudera.crunch" % "crunch" % "0.2.4" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u3" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
/*  "org.apache.hbase" % "hbase" % "0.90.3-cdh3u3" % "provided" excludeAll(
    ExclusionRule(organization = "org.apache.hadoop"),
    ExclusionRule(organization = "commons-logging"),
    ExclusionRule(organization = "com.google.guava"),
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j")
  ),*/
  "junit" % "junit" % "4.8.1" % "test",
  "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test"
)

parallelExecution in Test := false

assemblySettings

