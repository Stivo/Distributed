import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random
import scala.collection.mutable
import java.io.File

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

trait VectorsProg extends VectorImplOps with ComplexBase with ApplicationOps with SparkVectorOps {

  def strings(x: Rep[Unit]) = {
    val s = getArgs(0)
    val sc = unit("asdf")
    println(sc.length)
    println(s.length)
    println(s.trim.trim)

    println(sc.contains("as"))
    println(unit("asdf").split("s"))
  }

  def join(x: Rep[Unit]) = {
    val users = Vector(getArgs(0))
      .map(x => User(25, x, 35))
      .map(x => (x.userId, x))
      .filter(_._2.age == 35)
    val addresses = Vector(getArgs(1))
      .map(x => Address(25, x, 1003, "Lsn"))
      .map(x => (x.userId, x))
      .filter(_._2.street != "fff")
    val joined = users.join(addresses)
    joined.map { x => x._2._1.name + " " + x._2._2.city }
      .save(getArgs(2))
  }

  def nested(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1
      .map(x => N2(x, 558))
      //    .map(x => N2(x.n2id, 238))
      .map(x => if (x.n2id != "asdf") N1(x, x.n2id, 38) else N1(x, x.n2id + "1", 355))
      //      .map(x => if (x.matches("asdf")) N2(x, 5) else N2(x, 7))
      //            .map(x => N1(x, x.n2id, 38))
      //      .filter(_.n1Junk == 38)
      //            .filter(_.n2id=="asdf")
      .filter(_.n1Junk != 30)
      //            .map(_.n2id)
      .map(_.n2)
      .save(getArgs(1))
  }

  def logEntry(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.map(LogEntry(1L, 3.5, _))
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      //      .filter(_.url.matches("asdf"))
      //      .filter(_.request == 1L)
      //      .map(x => (unit(0), x))
      //      .filter(_._1 > 0)
      //      .map(_._2.url)
      //      .map(_.url)
      .map(x => (x._1.url, x._2))
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
    words1.filter(_.matches("\\d+")).map(_.toInt).map(_ + 3).cache
      .save(getArgs(1))
    //)(0)
    unit(())
  }

  def simple2(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.map { x => (x, unit(1)) }
      .filter(!_._1.matches("asdf"))
      .map(_._2)
      .save(getArgs(1))
    //)(0)
    unit(())
  }

  def wordCount(x: Rep[Unit]) = {
    val words1 = Vector("words1")
    val words2 = Vector("words2")
    val wordsInLine = words1 ++ words2 //.flatMap( _.split(" ").toSeq)
    //    words.map(_.contains(" ")).save("lines with more than one word")
    val wordsTupled = wordsInLine.map((_, unit(1)))
    val wordsGrouped = wordsTupled.groupByKey
    val counted = wordsGrouped.reduce(_ + _)
    counted.save("wordcounts")
  }

  def findLogEntry(x: Rep[Unit]) = {
    val words = Vector("words1")
    //    words.map(_.contains(" ")).save("lines with more than one word")
    val wordsTupled = words.map(x => (x, LogEntry.parse(x, "\\s")))
    val wordsGrouped = wordsTupled.groupByKey
    val counted = wordsGrouped.reduce((x, y) => if (x.request > y.request) x else y)
    counted.map(x => x._2.url)
      .save("logEntries")
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

trait CodeGenerator {

  val pairs = mutable.HashMap[PrintWriter, StringWriter]()

  def setUpPrintWriter() = {
    val sw = new StringWriter()
    var pw = new PrintWriter(sw)
    pairs += pw -> sw
    pw
  }

  def writeToProject(pw: PrintWriter, projName: String, filename: String) {
    writeToFile(pw, "%s/src/main/scala/generated/%s.scala".format(projName, filename))
  }

  def writeToFile(pw: PrintWriter, dest: String) {
    val x = new File(dest.reverse.dropWhile(_ != '/').reverse)
    x.mkdirs
    println(x + " " + dest)
    val fw = new FileWriter(dest)
    fw.write(getContent(pw))
    fw.close
  }

  def getContent(pw: PrintWriter) = {
    pw.flush
    val sw = pairs(pw)
    sw.toString
  }

  def release(pw: PrintWriter) {
    pairs -= pw
    pw.close
  }

}

class TestVectors extends Suite with CodeGenerator {

  def testSpark {
    try {
      println("-- begin")

      val dsl = new VectorsProg with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
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

  def testScoobi {
    try {
      println("-- begin")

      val dsl = new VectorsProg with VectorImplOps with ComplexStructExp with ApplicationOpsExp with SparkVectorOpsExp

      val pw = setUpPrintWriter
      val codegen = new ScoobiGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.findLogEntry, "g", pw)

      writeToProject(pw, "scoobi", "ScoobiGenerated")
      release(pw)
      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
}

