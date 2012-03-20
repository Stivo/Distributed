import java.io.PrintWriter
import java.io.StringWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{Base, StructExp, PrimitiveOps}

trait ComplexBase extends Base {
  
  class Complex
  
  def Complex(re: Rep[Double], im: Rep[Double]): Rep[Complex]
  def infix_re(c: Rep[Complex]): Rep[Double]
  def infix_im(c: Rep[Complex]): Rep[Double]
  def infix_abs(c: Rep[Complex]): Rep[Double]
}

trait ComplexStructExp extends ComplexBase with StructExp with PrimitiveOps {

  def Complex(re: Rep[Double], im: Rep[Double]) = struct[Complex](List("Complex"), Map("re"->re, "im"->im, "abs" -> im))
  def infix_re(c: Rep[Complex]): Rep[Double] = field[Double](c, "re")
  def infix_im(c: Rep[Complex]): Rep[Double] = field[Double](c, "im")
  def infix_abs(c: Rep[Complex]): Rep[Double] = field[Double](c, "abs")
}

trait VectorsProg extends VectorImplOps with ComplexBase {
  
  def fields(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1 //.filter(_.matches("\\d+"))
    .map(x => Complex(3, x.toInt))
//    .filter(_._2.im > 3)
//    .filter(_._1 == 0)
    .filter(_.abs > 3)
    .map(x => Complex(x.re, 3))
    .map(_.im)
//    .map(x => x._2.re)
    .save(getArgs(1))
    //)(0)
    unit(())
  }
  
  def simple(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.filter(_.matches("\\d+")).map(_.toInt).map(_ + 3)
      .save(getArgs(1))
    //)(0)
    unit(())
  }

  def simple2(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.map{ x=> (x,unit(1))}
    .filter(_._2 > 5)
    .map(_._1)
    .save(getArgs(1))
    //)(0)
    unit(())
  }
  
  def twoStage(x: Rep[Unit]) = {
    val words1 = Vector("words1")
    val words2 = Vector("words2")
    val words3 = Vector("words3")
    val wordsInLine = words1 ++ words2 ++ words3 //.flatMap( _.split(" ").toSeq)
    //    words.map(_.contains(" ")).save("lines with more than one word")
    val wordsTupled = wordsInLine.map((_, unit(1)))
    val wordsGrouped = wordsTupled.groupByKey
    val counted = wordsGrouped.reduce(_ + _)
    counted.save("wordcounts")
    val inverted = counted.map(x => (x._2, x._1))
    inverted.map(x => (unit(0), x))
      .groupByKey
      .reduce((x, y) => if (x._1 > y._1) x else y)
      .map(_._2)
      .save("most common word")
    //    val invertedGrouped = inverted.groupByKey
    //    val invertedGrouped2 = Vector[(Int, String)]("Asdf").groupByKey

    //    val added = invertedGrouped++invertedGrouped2
    //    invertedGrouped
    //    invertedGrouped.save("inverted")
  }

}

class TestVectors extends Suite {

  def testVectors {
    try {
      println("-- begin")

      val dsl = new VectorsProg with VectorImplOps with SparkVectorOpsExp with ComplexStructExp

      val sw = new StringWriter()
      var pw = new PrintWriter(sw)
            pw = new PrintWriter(System.out)
      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.fields, "g", pw)

      //      val dest = "/home/stivo/master/spark/examples/src/main/scala/spark/examples/SparkGenerated.scala"
      //      val fw = new FileWriter(dest)
      //      fw.write(writer.toString)
      //      fw.close

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
}

