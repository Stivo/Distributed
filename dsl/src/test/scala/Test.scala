import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }

trait ComplexBase extends Base {

  class Complex

  def Complex(re: Rep[Double], im: Rep[Double]): Rep[Complex]
  def infix_re(c: Rep[Complex]): Rep[Double]
  def infix_im(c: Rep[Complex]): Rep[Double]
  def infix_abs(c: Rep[Complex]): Rep[Double]
}

trait ComplexStructExp extends ComplexBase with StructExp with PrimitiveOps {

  def Complex(re: Rep[Double], im: Rep[Double]) = struct[Complex](List("Complex"), Map("re" -> re, "im" -> im, "abs" -> im))
  def infix_re(c: Rep[Complex]): Rep[Double] = field[Double](c, "re")
  def infix_im(c: Rep[Complex]): Rep[Double] = field[Double](c, "im")
  def infix_abs(c: Rep[Complex]): Rep[Double] = field[Double](c, "abs")
}

trait VectorsProg extends VectorImplOps with ComplexBase with ApplicationOps {

  def nested(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1
      //.map(x => N2(x, 558))
      //    .map(x => N2(x.n2id, 238))
      //      .map(x => if (x.n2junk > 5) N1(x, x.n2id, 38) else N1(x, x.n2id+1, 355))
      .map(x => if (x.matches("asdf")) N2(x, 5) else N2(x, 7))
      //    .map(x => N1(x, x.n2id, 38))
      //            .filter(_.n2.n2id=="asdf")
      //      .filter(_.n1Junk != 30)
      .map(_.n2junk).save(getArgs(1))
  }

  def logEntry(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.map(LogEntry(1L, 3.5, _))
      .filter(_.url.matches("asdf"))
      //      .filter(_.request == 1L)
      //      .map(x => (unit(0), x))
      //      .filter(_._1 > 0)
      //      .map(_._2.url)
      .map(_.url)
      .save(getArgs(1))
  }

  def fields(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1 //.filter(_.matches("\\d+"))
      .map(x => Complex(3, x.toInt))
      //    .filter(_._2.im > 3)
      //    .filter(_._1 == 0)
      .filter(_.abs > 3)
      .map(x => Complex(x.re, x.im))
      .map(x => x.im + x.re)
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
    words1.map { x => (x, unit(1)) }
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

      val dsl = new VectorsProg with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      val sw = new StringWriter()
      var pw = new PrintWriter(sw)
      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.nested, "g", pw)

      pw.flush
      //      println(sw.toString)

      val dest = "spark/src/main/scala/generated/SparkGenerated.scala"
      val fw = new FileWriter(dest)
      fw.write(sw.toString)
      fw.close

      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
}

