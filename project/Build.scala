import sbt._
import Keys._
//import com.typesafe.sbtscalariform.ScalariformPlugin

object HelloBuild extends Build {

    val lmsproj = RootProject(uri("./virtualization-lms-core/"))

    val LMS_Key = "EPFL" % "lms_2.10.0-M1-virtualized" % "0.2"

    val virtScala = "2.10.0-M1-virtualized" //"2.10.0-virtualized-SNAPSHOT"

    lazy val default = Project(id = "default",
                            base = file("."),
                            settings = Project.defaultSettings) aggregate(dsl)

   lazy val spark = Project(id = "spark",
                            base = file("spark"),
                            settings = Project.defaultSettings) // ++ formatSourceSettings) 
				//.dependsOn(ProjectRef(uri("git://github.com/mesos/spark.git#master"),"core"))

   //lazy val formatSourceSettings = seq(ScalariformPlugin.scalariformSettings: _*)



    lazy val dsl = Project(id = "dsl",
                            base = file("dsl"),
                            settings = Project.defaultSettings) //++ Seq(libraryDependencies += LMS_Key))

			//.dependsOn(RootProject(uri("git://github.com/Stivo/virtualization-lms-core.git#merge-checkpoint")))
			.dependsOn(lmsproj)

//    lazy val lms = Project(id = "lms", base=file("    


}
