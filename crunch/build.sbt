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
  "com.cloudera.crunch" % "crunch" % "0.3.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)

parallelExecution in Test := false

assemblySettings

