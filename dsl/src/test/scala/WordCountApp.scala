import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait WordCountApp extends DListProgram with ApplicationOps with SparkDListOps with StringAndNumberOps {

  def wikiArticleWordcount2012(x: Rep[Unit]) = {

    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map(x => "\\n" + x.plaintext)
      .map(_.replaceAll("""\[\[.*?\]\]""", " "))
      .flatMap(_.replaceAll("""\\[nNt]""", " ").split("[^a-zA-Z0-9']+").toSeq)
      .filter(x => x.length > 1)
      .map(x => if (x.matches("^((left|right)*(thumb)(nail|left|right)*)+[0-9A-Z].*?")) x.replaceAll("((left|right)*thumb(nail|left|right)*)+", "") else x)
      .map(x => (x, unit(1)))
      .groupByKey(getArgs(2).toInt)
      .reduce(_ + _)
      .save(getArgs(1))
    unit(())
  }


}

class WordCountAppGenerator extends CodeGeneratorTestSuite {

  val appname = "WordCountApp"
  
  // format: OFF
  /**
   * Variants:
   * LF+IN: Loop fusion, inlining, and field reduction
   * RF: The regex frontend. No fast splitter, no fast regex. Patterns are reused though.
   * FS: The regex frontend, with fast splitter, no fast regex
   * FR: The regex frontend, with the fast regex.
   * v0:
   *  		LF+IN	RF	FS	FR
   * v0:	-	-	-	-
   * v1:	x	-	-	-
   * v2:	x	x	-	-
   * v3:	x	x	x	-
   * v4:	x 	x	x	x
   */
  // format: ON
  def testBoth {
    tryCompile {
      println("-- begin")
      var fusionEnabled = false
      val dsl = new WordCountApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp
      val codegenSpark = new SparkGen { val IR: dsl.type = dsl }
      val codegenScoobi = new ScoobiGen { val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = fusionEnabled
      }
//      codegenScoobi.useWritables = true;
      val codegenCrunch = new CrunchGen { val IR: dsl.type = dsl }
      val list = List(codegenSpark, codegenScoobi, codegenCrunch)
      def writeVersion(version: String) {
//        if (version != "v0") return
        val func = dsl.wikiArticleWordcount2009 _
        for (gen <- list) {
          val versionDesc = version+ (gen match {
            case x: Versioned => x.version
            case _ => ""
          })
          var pw = setUpPrintWriter
          gen.emitProgram(func, appname, pw, versionDesc)
          writeToProject(pw, gen.getProjectName, appname, versionDesc, codegenSpark.lastGraph)
        }
      }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.inlineInLoopFusion = false
        codegen.loopFusion = false
      }
      fusionEnabled = false
      dsl.useFastSplitter = false
      dsl.disablePatterns = true
      dsl.useFastRegex = false
      writeVersion("v0")
      
      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
        codegen.inlineInLoopFusion = true
        codegen.loopFusion = true
      }
      fusionEnabled = true
      writeVersion("v1")

      dsl.disablePatterns = false
      writeVersion("v2")
      
      dsl.useFastSplitter = true
      writeVersion("v3")
      
      dsl.useFastRegex = true
      writeVersion("v4")

      println("-- end")
    }
  }

}
