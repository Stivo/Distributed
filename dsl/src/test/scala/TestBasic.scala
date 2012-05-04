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

  def Complex(re: Rep[Double], im: Rep[Double]) = struct[Complex](ClassTag[Complex]("Complex"), immutable.ListMap("re"->re, "im"->im))
  def infix_re(c: Rep[Complex]): Rep[Double] = field[Double](c, "re")
  def infix_im(c: Rep[Complex]): Rep[Double] = field[Double](c, "im")
  
}

trait DListsProg extends DListProgram with ComplexBase{

 
  def simple(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    words1.map(x => (Complex(x.toDouble, 5.0), unit("asdf")))
    .map(_._1.im)
//    .save(getArgs(1))
    words1.map(x => (x, unit(1))).groupByKey.save(getArgs(1))
//    words1
//    //.filter(_.matches("\\d+"))
//    .map(_.toInt)
   
    
    //)(0)
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

class TestDLists2 extends Suite with CodeGenerator {

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
  /*
  def testSpark {
    try {
      println("-- begin")

      val dsl = new DListsProg with DListImplOps with ComplexStructExp with ApplicationOpsExp with SparkDListOpsExp

      val codegen = new SparkGenDList { val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.findLogEntry, "g", pw)

      writeToProject(pw, "spark", "SparkGenerated")
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

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp

      val pw = setUpPrintWriter
      val codegen =  new BaseCodeGenerator with ScoobiGenDList { val IR: dsl.type = dsl }
      codegen.withStream(pw) {
      codegen.emitSource(dsl.simple, "g", pw)
      }
      writeToProject(pw, "scoobi", "ScoobiGenerated")
      println(getContent(pw))
      release(pw)
      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
 
}

