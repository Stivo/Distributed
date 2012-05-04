import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait PageCountApp extends VectorImplOps with ApplicationOps with SparkVectorOps {

  def parse(x: Rep[String]) = {
    val splitted = x.split("\\s")

    //aa.b Special:Contributions/ShakataGaNai 1 5907
    val lang_proj = splitted(0)
    val lang = if (lang_proj.contains(".")) {
      val both = lang_proj.split("\\.")
      both(0)
    } else {
      lang_proj
    }
    val proj = if (lang_proj.contains(".")) {
      val both = lang_proj.split("\\.")
      both(1)
    } else {
      "w"
    }
    PageCountEntry(lang, proj, splitted(1), splitted(2).toLong, splitted(3).toLong)
  }

  def statistics(x: Rep[Unit]) = {
    val read = Vector(getArgs(0))
    val works = read.map(x => (x, unit(1)))
    //.groupByKey.save(folder+"asdf")
    val parsed = read.map(parse)
    val byLang = parsed.map(x => (x.site, List(x.language)))
    byLang.groupByKey.reduce(_ ++ _).filter(_._2.size > 3).save(getArgs(1) + "/multipleLangs")
    val requests = parsed.map(x => (unit("requests"), x.number))
    val grouped = requests.groupByKey
    grouped.reduce(_ + _).save(getArgs(1) + "/requests")
    //    parsed.save(folder+"/output/")
    unit(())
  }

}

class PageCountAppGenerator extends Suite with CodeGenerator {

  val appname = "PageCountApp"
  val unoptimizedAppname = appname + "_Orig"

  def testSpark {
    try {
      println("-- begin")

      val dsl = new PageCountApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.statistics, appname, pw)
      writeToProject(pw, "spark", appname)
      release(pw)

      //      val codegenUnoptimized = new { override val allOff = true } with SparkGenVector { val IR: dsl.type = dsl }
      //      codegenUnoptimized.reduceByKey = true
      //      pw = setUpPrintWriter
      //      codegenUnoptimized.emitSource(dsl.statistics, unoptimizedAppname, pw)
      //      writeToProject(pw, "spark", unoptimizedAppname)
      //      release(pw)

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

      val dsl = new PageCountApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      var pw = setUpPrintWriter
      val codegen = new ScoobiGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.statistics, appname, pw)
      writeToProject(pw, "scoobi", appname)
      release(pw)

      //      val codegenUnoptimized = new { override val allOff = true } with ScoobiGenVector { val IR: dsl.type = dsl }
      //      pw = setUpPrintWriter
      //      codegenUnoptimized.emitSource(dsl.statistics, unoptimizedAppname, pw)
      //      writeToProject(pw, "scoobi", unoptimizedAppname)
      //      release(pw)

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }

}
