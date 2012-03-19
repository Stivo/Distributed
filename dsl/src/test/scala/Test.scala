import java.io.PrintWriter
import java.io.StringWriter
import ch.epfl.distributed._
import org.scalatest._

trait VectorsProg extends VectorImplOps {
  def simple(x: Rep[Unit]) = {
    val words1 = Vector(getArgs(0))
    words1.map(_.toInt).map(_ + 3)
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

      val dsl = new VectorsProg with VectorImplOps with SparkVectorOpsExp // ScalaGenVector

      val sw = new StringWriter()
      var pw = new PrintWriter(sw)
      //      pw = new PrintWriter(System.out)
      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.twoStage, "g", pw)

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

