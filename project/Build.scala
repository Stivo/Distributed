import sbt._
import Keys._
import com.typesafe.sbtscalariform.ScalariformPlugin
import scalariform.formatter.preferences._

object HelloBuild extends Build {

   ScalariformPlugin.ScalariformKeys.preferences := FormattingPreferences().setPreference(SpaceBeforeColon, true)

   val lmsproj = RootProject(uri("./virtualization-lms-core/"))

   val virtScala = "2.10.0-M1-virtualized"

   lazy val default = Project(id = "default",
                            base = file("."),
                            settings = Project.defaultSettings ++ Seq(helloTask)) aggregate(dsl)

   lazy val spark = Project(id = "spark",
                            base = file("spark"),
                            settings = Project.defaultSettings ++ formatSourceSettings) 
				//.dependsOn(ProjectRef(uri("git://github.com/mesos/spark.git#master"),"core"))

   lazy val formatSourceSettings = seq(ScalariformPlugin.scalariformSettings: _*)

   lazy val dsl = Project(id = "dsl",
                            base = file("dsl"),
                            settings = Project.defaultSettings)
			.dependsOn(lmsproj)

   val dotGen = TaskKey[Unit]("dotgen", "Runs the dot generation")
   val helloTask = dotGen := {
	 "bash updateDot.sh" .run
   }

}
