import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps, LiftNumeric }
import scala.util.Random

trait TpchQueriesApp extends DListProgram with ApplicationOps {

  def loadTest(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    val parsed = read.map(x => LineItem.parse(x, "\\|"))
    parsed
      .filter(_.l_linestatus == 'F')
      .map(x => (x.l_receiptdate, x.l_comment))
      .save(getArgs(1))
  }

  def query3nephele(x: Rep[Unit]) = {
    val limit = getArgs(1).toInt
    val date = getArgs(2).toDate
    val lineitems = DList(getArgs(0) + "/lineitem.tbl")
      .map(x => LineItem.parse(x, "\\|"))
    val orders = DList(getArgs(0) + "/orders.tbl")
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

  def query12(x: Rep[Unit]) = {
    // read arguments
    val inputFolder = getArgs(0)
    val outputFolder = getArgs(1)
    val date = getArgs(2).toDate
    val shipMode1 = getArgs(3)
    val shipMode2 = getArgs(4)

    // read and parse tables
    val lineitems = DList(getArgs(0) + "/lineitem.tbl")
      .map(x => LineItem.parse(x, "\\|"))
    val orders = DList(getArgs(0) + "/orders.tbl")
      .map(x => Order.parse(x, "\\|"))

    // filter the line items
    val filteredLineitems = lineitems
      .filter(x => x.l_shipmode == shipMode1 || x.l_shipmode == shipMode2)
      .filter(x => date <= x.l_receiptdate)
      .filter(x => x.l_shipdate < x.l_commitdate)
      .filter(x => x.l_commitdate < x.l_receiptdate)
      .filter(x => x.l_receiptdate < date + (1, 0, 0))
    // perform the join
    val orderTuples = orders.map(x => (x.o_orderkey, x))
    val lineItemTuples = filteredLineitems.map(x => (x.l_orderkey, x))
    val joined = lineItemTuples.join(orderTuples)
    // prepare for aggregation
    val joinedTupled = joined.map {
      x =>
        val prio = x._2._2.o_orderpriority;
        val isHigh = prio.startsWith("1") || prio.startsWith("2");
        val count = if (isHigh) 1 else 0
        val part2: Rep[(Int, Int)] = (count, 1 - count)
        (x._2._1.l_shipmode, count)
    }
    // aggregate and save
    joinedTupled.save(getArgs(1))
    //    val reduced = joinedTupled.groupByKey.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    //    reduced.map {
    //      x =>
    //        "shipmode " + x._1 + ": high " + x._2._1 + ", low " + x._2._2
    //    }.save(getArgs(1))
  }

  /*
  def tupleProblem(x: Rep[Unit]) = {
    val lineitems = DList(getArgs(0) + "/lineitem.tbl")
      .map(x => LineItem.parse(x, "\\|"))
    val tupled = lineitems.map(x => ((x.l_linenumber, x.l_orderkey), x.l_comment))
    tupled
      .save(getArgs(3))
  }
  */

}

class TpchQueriesAppGenerator extends Suite with CodeGenerator {

  val appname = "TpchQueries"
  val unoptimizedAppname = appname + "_Orig"

  /*
  def testSpark {
    try {
      println("-- begin")

      val dsl = new TpchQueriesApp with DListImplOps with ApplicationOpsExp with SparkDListOpsExp
      // val codegen = new { override val allOff = true } with SparkGenDList { val IR: dsl.type = dsl }
      val codegen = new SparkGenDList { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.query12, appname, pw)
      writeToProject(pw, "spark", appname)
      release(pw)

      val typesDefined = codegen.types.keys
      val codegenUnoptimized = new { override val allOff = true } with SparkGenDList { val IR: dsl.type = dsl }
      codegenUnoptimized.skipTypes ++= typesDefined
      codegenUnoptimized.reduceByKey = true
      pw = setUpPrintWriter
      codegenUnoptimized.emitSource(dsl.query12, unoptimizedAppname, pw)
      writeToProject(pw, "spark", unoptimizedAppname)
      release(pw)

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
  */
  def testScoobi {
    try {
      println("-- begin")

      var pw = setUpPrintWriter

      val dsl = new TpchQueriesApp with DListProgramExp with ApplicationOpsExp
      val codegen = new BaseCodeGenerator with ScoobiGenDList { val IR: dsl.type = dsl }

      codegen.emitSource(dsl.query12, appname, pw)
      writeToProject(pw, "scoobi", appname)
      release(pw)

      //      val typesDefined = codegen.types.keys
      //      val codegenUnoptimized = new { override val allOff = true } with ScoobiGenDList { val IR: dsl.type = dsl }
      //      codegenUnoptimized.skipTypes ++= typesDefined
      //      pw = setUpPrintWriter
      //      codegenUnoptimized.emitSource(dsl.query12, unoptimizedAppname, pw)
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