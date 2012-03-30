package examples.weblog

//import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._
import java.util.Date
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import com.nicta.scoobi.{ Emitter, WireFormat, DList, DoFn }

object WebLogAnalyzer {

  val format = new SimpleDateFormat("yy/MM/dd HH:mm:ss.SSS")
  def getTime = format.format(new Date) + " "
  def log(s: String) {
    System.err.println(getTime + s)
  }

  def filter[A: Manifest: WireFormat](dlist: DList[A], p: A => Boolean): DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() = {
        log("Starting filter")
      }
      def process(input: A, emitter: Emitter[A]) = { if (p(input)) { emitter.emit(input) } }
      def cleanup(emitter: Emitter[A]) = {
        log("Ending filter")
      }
    }
    dlist.parallelDo(dofn)
  }
  def matches(pattern: Pattern, input: String) = {
    val m = pattern.matcher(input);
    m.matches()
  }
  val pattern1 = Pattern.compile("18\\d{2}")

  def main(args: Array[String]) = withHadoopArgs(args) { a =>
    // some extracts from this file for reference
    """
266407760 1199314252.550 http://upload.wikimedia.org/wikipedia/commons/thumb/b/b3/RAF_roundel.svg/75px-RAF_roundel.svg.png -
266407764 1199314252.591 http://es.wikipedia.org/skins-1.5/common/images/magnify-clip.png -
266407762 1199314252.549 http://upload.wikimedia.org/fundraising/2007/people-meter-ltr.png -
266407763 1199314252.545 http://upload.wikimedia.org/fundraising/2007/red-button-left.png -
   """
    val (inputPath, outputPath) = (args(0), args(1))
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val years = lines.flatMap(_.split(" ")).flatMap(_.split("/"))
      .filter(_.matches("18\\d{2}"))

    // to allow measuring of the time in the mapper phase
    val all = filter(lines, { x: String => x.length > 0 })
    //	val dates = all.flatMap(_.split(" ")).filter(_.contains("/")).flatMap(_.split("/"))
    ////		.filter(_.matches("18\\d{2}"))
    //		.filter(matches(pattern1, _))
    val partCounts = all.flatMap(_.split("\\s+"))
      .filter(_.contains("/"))
      .flatMap(_.split("/"))
      .filter(_.length > 10)
      .map(x => (x, 1))
      .groupByKey
      .combine((x: Int, y: Int) => x + y)
    //	.filter(_._2>1000)
    // filter-xx mode
    //	val length = all.filter(_.length>0).filter(_.length>4).filter(_.length>8)
    //	.filter(_.length>12).filter(_.length>16).filter(_.length>20).filter(_.length>24)
    //	.filter(_.length>28).filter(_.length>32).filter(_.length>36).filter(_.length>40)
    //	.filter(_.length>44).filter(_.length>48).filter(_.length>52).filter(_.length>56)
    //	.filter(_.length>60).filter(_.length>64).filter(_.length>68).filter(_.length>72)
    //	.filter(_.length>76).filter(_.length>80).filter(_.length>84).filter(_.length>88)
    //	.filter(_.length>92).filter(_.length>96).filter(_.length>100).filter(_.length>104)
    //	.filter(_.length>108).filter(_.length>112).filter(_.length>116).filter(_.length>120)
    //	.filter(_.length>124).filter(_.length>128).filter(_.length>132).filter(_.length>136)
    //	.filter(_.length>140).filter(_.length>144).filter(_.length>148).filter(_.length>152)
    //	.filter(_.length>156).filter(_.length>200)
    //	val lengths : DList[(Int, Iterable[Int])] = all.map(x => (x.length, 1)).groupByKey
    //	val combined :  DList[(Int, Int)]= lengths.combine((_+_))
    //	.map(_.dropRight(2)).map(_.split(" ")).map(_(2))
    //	.filter(_.contains("en.wikipedia")).filter(_.endsWith("svg"))
    //val pics = restLines.map(_.split("/").last)
    // Firstly we load up all the (new-line-separated) words into a DList
    /*  nasa log code

    val restLines = lines.filter(!_.contains(" 200 ")).filter(_.contains("aol.com "))
	val urls = restLines.map(_.split("\"")(1)).filter(_.contains(".gif "))
*/
    //val date = lines.map(x =>x.slice(x.indexOf("["), x.indexOf("]")))
    // We can evaluate this, and write it to a text file
    if (args.length > 2) {
      DList.persist(TextOutput.toTextFile(years, outputPath));
    } else {
      //    DList.persist(TextOutput.toTextFile(length, outputPath + "/word-results"));
      DList.persist(TextOutput.toTextFile(partCounts, outputPath));
    }
  }

}
