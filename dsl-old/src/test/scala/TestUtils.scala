import java.io.{ StringWriter, PrintWriter, File, FileWriter }
import scala.collection.mutable

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