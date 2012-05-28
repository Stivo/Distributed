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

    unit(())
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
    unit(())
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
        (x._2._1.l_shipmode, part2)
    }

    // aggregate and save
    val reduced = joinedTupled.groupByKey.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    reduced.map {
      x =>
        "shipmode " + x._1 + ": high " + x._2._1 + ", low " + x._2._2
    }.save(getArgs(1))

    unit(())
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

class TpchQueriesAppGenerator extends CodeGeneratorTestSuite {

  val appname = "TpchQueries"
  val unoptimizedAppname = appname + "_Orig"

  // format: OFF
  /**
   * Variants:
   *  		FR	LF 	IN
   * v0:	-	-	-
   * v1:	x	-	-
   * v2:	-	x	-
   * v3:	-	x	x
   * v4:	x 	x	-
   * v5:	x	x 	x
   */
  // format: ON
  def testBoth {
    tryCompile {
      println("-- begin")
      var applyFusion = true
      val dsl = new TpchQueriesApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp {
        override val verbosity = 1
      }
      val codegenSpark = new SparkGen {
        val IR: dsl.type = dsl
        import IR._
        override def shouldApplyFusion(currentScope: List[Stm])(result: List[Exp[Any]]): Boolean = applyFusion
      }
      val codegenScoobi = new ScoobiGen {
        val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = applyFusion
      }
      val list = List(codegenSpark, codegenScoobi)
      def writeVersion(version: String) {
        var pw = setUpPrintWriter
        codegenSpark.emitProgram(dsl.query12, appname, pw, version)
        writeToProject(pw, "spark", appname, version, codegenSpark.lastGraph)
        release(pw)
        //        pw = setUpPrintWriter
        //        codegenScoobi.emitProgram(dsl.query12, appname, pw, version)
        //        writeToProject(pw, "scoobi", appname, version, codegenScoobi.lastGraph)
        //        release(pw)
      }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.loopFusion = false
        codegen.inlineClosures = true
        codegen.inlineInLoopFusion = false
      }
      writeVersion("v0")

      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      writeVersion("v1")

      list.foreach { codegen =>
        codegen.loopFusion = true
        codegen.inlineClosures = false
      }
      writeVersion("v4")

      list.foreach { _.inlineInLoopFusion = true }
      writeVersion("v5")

      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
      }
      writeVersion("v3")

      list.foreach { _.inlineInLoopFusion = false }
      writeVersion("v2")
      println("-- end")
    }

  }

}
