import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps, LiftNumeric }
import scala.util.Random

trait TpchQueriesApp extends VectorImplOps with ApplicationOps with SparkVectorOps {

  def loadTest(x: Rep[Unit]) = {
    val read = Vector(getArgs(0))
    val parsed = read.map(x => LineItem.parse(x, "\\|"))
    parsed
      .filter(_.l_linestatus == 'F')
      .map(x => (x.l_receiptdate, x.l_comment))
      .save(getArgs(1))
  }

  def query3nephele(x: Rep[Unit]) = {
    val limit = getArgs(1).toInt
    val date = getArgs(2).toDate
    val lineitems = Vector(getArgs(0) + "/lineitem.tbl")
      .map(x => LineItem.parse(x, "\\|"))
    val orders = Vector(getArgs(0) + "/orders.tbl")
      .map(x => Order.parse(x, "\\|"))
    val filteredOrders = orders
      .filter(x => x.o_custkey < limit)
      .filter(x => date < x.o_orderdate)
    val lineItemTuples = lineitems.map(x => (x.l_orderkey, x))
    val orderTuples = filteredOrders.map(x => (x.o_orderkey, x))
    val joined = lineItemTuples.join(orderTuples)
    val tupled = joined.map { x => val y: Rep[(Int, Int)] = (x._2._1.l_orderkey, x._2._2.o_shippriority); (y, x._2._1.l_extendedprice) }
    val grouped = tupled.groupByKey
    grouped.reduce((x, y) => x + y)
      .save(getArgs(3))
  }

  def tupleProblem(x: Rep[Unit]) = {
    val lineitems = Vector(getArgs(0) + "/lineitem.tbl")
      .map(x => LineItem.parse(x, "\\|"))
    val tupled = lineitems.map(x => ((x.l_linenumber, x.l_orderkey), x.l_comment))
    tupled
      .save(getArgs(3))
  }

}

class TpchQueriesAppGenerator extends Suite with CodeGenerator {

  val appname = "TpchQueries"
  val unoptimizedAppname = appname + "_Orig"

  def testSpark {
    try {
      println("-- begin")

      val dsl = new TpchQueriesApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp
      // val codegen = new { override val allOff = true } with SparkGenVector { val IR: dsl.type = dsl }
      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.query3nephele, appname, pw)
      writeToProject(pw, "spark", appname)
      release(pw)

      val typesDefined = codegen.types.keys
      val codegenUnoptimized = new { override val allOff = true } with SparkGenVector { val IR: dsl.type = dsl }
      codegenUnoptimized.skipTypes ++= typesDefined
      codegenUnoptimized.reduceByKey = true
      pw = setUpPrintWriter
      codegenUnoptimized.emitSource(dsl.query3nephele, unoptimizedAppname, pw)
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

      val dsl = new TpchQueriesApp with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      var pw = setUpPrintWriter
      val codegen = new ScoobiGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.query3nephele, appname, pw)
      writeToProject(pw, "scoobi", appname)
      release(pw)

      //      val typesDefined = codegen.types.keys
      //      val codegenUnoptimized = new { override val allOff = true } with ScoobiGenVector { val IR: dsl.type = dsl }
      //      codegenUnoptimized.skipTypes ++= typesDefined
      //      pw = setUpPrintWriter
      //      codegenUnoptimized.emitSource(dsl.query3nephele, unoptimizedAppname, pw)
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
