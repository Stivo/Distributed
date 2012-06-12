/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated.v1;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo

object WordCountApp {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: false
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v1.Registrator_WordCountApp")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")

    val sc = new SparkContext(sparkInputArgs(0), "WordCountApp")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x54 = x1(1);
    @inline
    def x1371(x49: Int, x50: Int) = {
      val x51 = x49 + x50;
      x51: Int
    }
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    // x3
    val x1662 = x3.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, Int]] {
        private[this] val buff = new Array[scala.Tuple2[java.lang.String, Int]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x215 = it.next // loop var x213;
            val x307 = x215.split("""	""", 5);
            val x308 = x307(4);
            val x1386 = """\n""" + x308;
            val x1565 = x1386.replaceAll("""\[\[.*?\]\]""", """ """);
            val x1566 = x1565.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
            val x1638 = x1566.split("""[^a-zA-Z0-9']+""", 0);
            val x1639 = x1638.toSeq;
            // x1639
            {
              val it = x1639.iterator
              while (it.hasNext) { // flatMap
                val x1641 = it.next // loop var x254;
                val x1642 = x1641.length;
                val x1643 = x1642 > 1;
                val x1655 = if (x1643) {
                  val x1644 = x1641.matches("""(thumb|left|right|\d+px){2,}""");
                  val x1645 = !x1644;
                  val x1651 = if (x1645) {
                    val x1646 = (x1641, 1);
                    buff(i) = x1646 // yield
                    i = i + 1
                    val x1647 = ()
                    x1647
                  } else {
                    val x1649 = () // skip;
                    x1649
                  }
                  x1651
                } else {
                  val x1653 = () // skip;
                  x1653
                }
              }
            }
          }

          start = 0
          end = if (i < buff.length) i else i - 1
        }

        override def hasNext(): Boolean = {
          if (start == end) load

          start != end
        }

        override def next = {
          if (start == end) load

          val res = buff(start)
          start += 1
          res
        }
      }
    })
    val x1663 = x1662.reduceByKey(x1371);
    val x1664 = x1663.saveAsTextFile(x54);

    System.exit(0)
  }
}
// Types that are used in this program

class Registrator_WordCountApp extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {

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
