import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random
import scala.collection.mutable
import java.io.File
import scala.collection.immutable
trait ComplexBase extends Base {

  class Complex

  def Complex(re: Rep[Double], im: Rep[Double]): Rep[Complex]
  def infix_re(c: Rep[Complex]): Rep[Double]
  def infix_im(c: Rep[Complex]): Rep[Double]
}

trait ComplexStructExp extends ComplexBase with StructExp {

  def Complex(re: Rep[Double], im: Rep[Double]) = struct[Complex](ClassTag[Complex]("Complex"), immutable.ListMap("re" -> re, "im" -> im))
  def infix_re(c: Rep[Complex]): Rep[Double] = field[Double](c, "re")
  def infix_im(c: Rep[Complex]): Rep[Double] = field[Double](c, "im")

}

trait DListsProg extends DListProgram with ComplexBase {

  def simple(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    words1.map(x => (Complex(x.toDouble, 5.0), unit("asdf")))
      .map(x => (x._1.im))
    //    .save(getArgs(1))

    val tupled = words1
    tupled.map(x => (x, unit(1))).groupByKey.save(getArgs(1))
    //    tupled.map(x => (x, unit(1))).groupByKey.save(getArgs(2))

    //    words1
    //    //.filter(_.matches("\\d+"))
    //    .map(_.toInt)

    //)(0)
    unit(())
  }

  def testJoin(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0) + "1")
    val words2 = DList(getArgs(0) + "2")
    val words1Tupled = words1.map(x => (x, Complex(x.toDouble, 3.0)))
    val words2Tupled = words2.map(x => (x, Complex(2.5, x.toDouble)))
    val joined = words1Tupled.join(words2Tupled)
    joined.map(x => x._1 + " " + x._2._1.re + " " + x._2._2.im)
      //    joined
      .save(getArgs(1))
    //    joined
    unit(())
  }

  def testWhile(x: Rep[Unit]) = {
    val nums = DList(getArgs(0)).map(_.toInt)
    var x = 0
    var quad = nums
    while (x < 5) {
      quad = quad.map(x => x + unit(2))
      x = x + 1
    }
    quad.save(getArgs(1))
    unit(())
  }

  /*
  def simple2(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    words1.map { x => (x, unit(1)) }
      .filter(!_._1.matches("asdf"))
      .map(_._2)
      .save(getArgs(1))
    //)(0)
    unit(())
  }
  */

}

class TestDLists2 extends CodeGeneratorTestSuite {

  /*
  def testPrinter {
    try {
      println("-- begin")

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp

      val codegen = new BaseCodeGenerator with ScalaGenDList { val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.emitProgram(dsl.simple, "test", pw)
      println(getContent(pw))
//      writeToProject(pw, "spark", "SparkGenerated")
      release(pw)
      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }

  }
  */

  def testSpark {
    tryCompile {
      println("-- begin")

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp with ApplicationOpsExp with SparkDListOpsExp

      val codegen = new SparkGenDList { val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.testJoin, "g", pw)

      writeToProject(pw, "spark", "SparkGenerated")
      release(pw)
      println("-- end")
    }
  }

  def testScoobi {
    tryCompile {
      println("-- begin")
      val pw = setUpPrintWriter

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp

      val codegen = new ScoobiGenDList { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.testJoin, "g", pw)
      writeToProject(pw, "scoobi", "ScoobiGenerated")
      //      println(getContent(pw))
      release(pw)
      println("-- end")
    }
  }

}

