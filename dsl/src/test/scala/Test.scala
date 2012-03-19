import java.io.PrintWriter
import java.io.StringWriter
import ch.epfl.distributed._
import org.scalatest._

trait VectorsProg extends VectorImplOps {
  def simple(x: Rep[Unit]) = {
    val words1 = Vector("words1")
    words1.map(_.toInt)
      .save("words1Out")
    unit("348")
    unit(())
  }

}

class TestVectors extends Suite {

  def testVectors {
    try {
      println("-- begin")

      val dsl = new VectorsProg with VectorImplOps with SparkVectorOpsExp // ScalaGenVector

      val codegen = new SparkGenVector { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.simple, "g", new PrintWriter(System.out))

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

