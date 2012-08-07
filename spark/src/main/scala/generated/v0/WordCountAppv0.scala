/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated.v0;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo
import ch.epfl.distributed.utils.Helpers.makePartitioner

object WordCountApp {
  // field reduction: false
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: false
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v0.Registrator_WordCountApp")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "WordCountApp")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x56 = x1(1);
    val x48 = x1(2);
    val x49 = x48.toInt;
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    @inline
    def x59(x4: (java.lang.String)) = {
      val x5 = x4.split("""	""", 5);
      val x6 = x5(0);
      val x7 = x6.toLong;
      val x8 = x5(1);
      val x9 = x5(2);
      val x10 = ch.epfl.distributed.datastruct.Date(x9);
      val x11 = x5(3);
      val x12 = x5(4);
      val x13 = new WikiArticle_0_1_2_3_4(x7, x8, x10, x11, x12);
      x13: WikiArticle
    }
    val x60 = x3.map(x59);
    @inline
    def x61(x16: (WikiArticle)) = {
      val x17 = x16.plaintext;
      val x18 = """\n""" + x17;
      x18: java.lang.String
    }
    val x62 = x60.map(x61);
    @inline
    def x63(x21: (java.lang.String)) = {
      val x22 = x21.replaceAll("""\[\[.*?\]\]""", """ """);
      x22: java.lang.String
    }
    val x64 = x62.map(x63);
    @inline
    def x65(x25: (java.lang.String)) = {
      val x26 = x25.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
      x26: java.lang.String
    }
    val x66 = x64.map(x65);
    @inline
    def x67(x29: (java.lang.String)) = {
      val x30 = x29.split("""[^a-zA-Z0-9']+""", 0);
      val x31 = x30.toSeq;
      x31: scala.collection.Seq[java.lang.String]
    }
    val x68 = x66.flatMap(x67);
    @inline
    def x69(x34: (java.lang.String)) = {
      val x35 = x34.length;
      val x36 = x35 > 1;
      x36: Boolean
    }
    val x70 = x68.filter(x69);
    @inline
    def x71(x39: (java.lang.String)) = {
      val x40 = x39.matches("""(thumb|left|right|\d+px){2,}""");
      val x41 = !x40;
      x41: Boolean
    }
    val x72 = x70.filter(x71);
    @inline
    def x73(x44: (java.lang.String)) = {
      val x45 = (x44, 1);
      x45: scala.Tuple2[java.lang.String, Int]
    }
    val x74 = x72.map(x73);
    @inline
    def x76(x51: Int, x52: Int) = {
      val x53 = x51 + x52;
      x53: Int
    }
    val x77 = x74.reduceByKey(x76 _, x49);
    val x78 = x77.saveAsTextFile(x56);

    System.exit(0)
  }
}
// Types that are used in this program
case class WikiArticle_0_1_2_3_4(override val pageId: Long, override val name: java.lang.String, override val updated: ch.epfl.distributed.datastruct.Date, override val xml: java.lang.String, override val plaintext: java.lang.String) extends WikiArticle {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("WikiArticle(")
    sb.append(pageId); sb.append(",");
    sb.append(name); sb.append(",");
    sb.append(updated); sb.append(",");
    sb.append(xml); sb.append(",");
    sb.append(plaintext); sb.append(",")
    sb.append(")")
    sb.toString()
  }
}
trait WikiArticle extends Serializable {
  def pageId: Long = throw new RuntimeException("Should not try to access pageId here, internal error")
  def name: java.lang.String = throw new RuntimeException("Should not try to access name here, internal error")
  def updated: ch.epfl.distributed.datastruct.Date = throw new RuntimeException("Should not try to access updated here, internal error")
  def xml: java.lang.String = throw new RuntimeException("Should not try to access xml here, internal error")
  def plaintext: java.lang.String = throw new RuntimeException("Should not try to access plaintext here, internal error")
}
class Registrator_WordCountApp extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[WikiArticle])
    kryo.register(classOf[WikiArticle_0_1_2_3_4])
    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}
/**
 * ***************************************
 * End of Spark Code
 * *****************************************
 */
