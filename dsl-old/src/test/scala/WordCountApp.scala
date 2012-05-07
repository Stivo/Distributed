import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait WordCountApp extends VectorImplOps with ApplicationOps with SparkVectorOps {

  def parse(x: Rep[String]): Rep[String] = {
    val splitted = x.split("\\s+")
    splitted.apply(2)
  }

  def statistics(x: Rep[Unit]) = {
    val read = Vector(getArgs(0))
    val parsed = read.map(parse)
      .filter(_.matches(".*?en\\.wiki.*?/wiki/.*"))
    //    .filter(_.contains("/wiki/"))
    val parts = parsed.map(_.split("/+").last)
      .filter(!_.matches("[A-Za-z_]+:(?!_).*"))
    parts
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .filter(_._2 >= 5)
      .save(getArgs(1))
    //    parsed.save(folder+"/output/")
    unit(())
  }

}

class WordCountAppGenerator extends Suite with CodeGenerator {

  val appname = "WordCountApp"
  val unoptimizedAppname = appname + "_Orig"

  def testSpark {
    try {
      println("-- begin")

      val dsl = new WordCountApp with VectorImplOps with ApplicationOpsExp with SparkVectorOpsExp
      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.statistics, appname, pw)
      writeToProject(pw, "spark", appname)
      release(pw)

      dsl.disablePatterns = true
      val codegenUnoptimized = new { override val allOff = true } with SparkGenVector with MoreIterableOpsCodeGen { val IR: dsl.type = dsl }
      codegenUnoptimized.reduceByKey = true
      pw = setUpPrintWriter
      codegenUnoptimized.emitSource(dsl.statistics, unoptimizedAppname, pw)
      writeToProject(pw, "spark", unoptimizedAppname)
      release(pw)

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }

  def testScoobi {
    try {
      println("-- begin")

      val dsl = new WordCountApp with VectorImplOps with ApplicationOpsExp with SparkVectorOpsExp

      var pw = setUpPrintWriter
      val codegen = new ScoobiGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.statistics, appname, pw)
      writeToProject(pw, "scoobi", appname)
      release(pw)

      dsl.disablePatterns = true
      val codegenUnoptimized = new { override val allOff = true } with ScoobiGenVector { val IR: dsl.type = dsl }
      pw = setUpPrintWriter
      codegenUnoptimized.emitSource(dsl.statistics, unoptimizedAppname, pw)
      writeToProject(pw, "scoobi", unoptimizedAppname)
      release(pw)

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }

}