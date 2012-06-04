import AssemblyKeys._ // put this at the top of the file

name := "scoobi-gen"

version := "0.1"

scalaVersion := "2.9.2"

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" excludeAll( // % "provided" 
   ExclusionRule(organization = "javax.servlet.jsp")
 )
 
resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/"

resolvers += "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Packaged Avro" at "http://nicta.github.com/scoobi/releases/"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if x.startsWith("javax/servlet") => MergeStrategy.first
    case x if x.startsWith("org/xmlpull") => MergeStrategy.first
    case x => old(x)
  }
}
