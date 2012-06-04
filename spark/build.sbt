//assemblySettings

name := "spark-gen"

scalacOptions += "-optimise"

scalaVersion := "2.9.1"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.4-SNAPSHOT"

libraryDependencies += "dk.brics.automaton" % "automaton" % "1.11-8"
