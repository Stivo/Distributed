import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait TpchQueriesApp extends VectorImplOps with ApplicationOps with SparkVectorOps {

  def loadTest(x: Rep[Unit]) = {
    val read = Vector(getArgs(0))
    val parsed = read.map(x => LineItem.parse(x, "\\|"))
    parsed
      .filter(_.l_linestatus == 'E')
      .map(x => (x.l_receiptdate, x.l_comment))
      .save(getArgs(1))
  }

}

class TpchQueriesAppGenerator extends Suite with CodeGenerator {

  val appname = "TpchQueries"
  val unoptimizedAppname = appname + "_Orig"

  def testSpark {
    try {
      println("-- begin")

      val dsl = new TpchQueriesApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.loadTest, appname, pw)
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

      val dsl = new TpchQueriesApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      var pw = setUpPrintWriter
      val codegen = new ScoobiGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.loadTest, appname, pw)
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
