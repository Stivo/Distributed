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
import ch.epfl.distributed.utils.Helpers.makePartitioner

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
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "WordCountApp")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x56 = x1(1);
    val x48 = x1(2);
    val x49 = x48.toInt;
    @inline
    def x1392(x51: Int, x52: Int) = {
      val x53 = x51 + x52;
      x53: Int
    }
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    // x3
    val x1683 = x3.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, Int]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[java.lang.String, Int]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x236 = it.next // loop var x234;
            val x328 = x236.split("""	""", 5);
            val x329 = x328(4);
            val x1407 = """\n""" + x329;
            val x1586 = x1407.replaceAll("""\[\[.*?\]\]""", """ """);
            val x1587 = x1586.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
            val x1659 = x1587.split("""[^a-zA-Z0-9']+""", 0);
            val x1660 = x1659.toSeq;
            // x1660
            {
              val it = x1660.iterator
              while (it.hasNext) { // flatMap
                val x1662 = it.next // loop var x275;
                val x1663 = x1662.length;
                val x1664 = x1663 > 1;
                val x1676 = if (x1664) {
                  val x1665 = x1662.matches("""(thumb|left|right|\d+px){2,}""");
                  val x1666 = !x1665;
                  val x1672 = if (x1666) {
                    val x1667 = (x1662, 1);
                    buff(i) = x1667 // yield
                    i = i + 1
                    val x1668 = ()
                    x1668
                  } else {
                    val x1670 = () // skip;
                    x1670
                  }
                  x1672
                } else {
                  val x1674 = () // skip;
                  x1674
                }
              }
            }
          }

          start = 0
          end = buff.length
        }

        override def hasNext(): Boolean = {
          if (start == end) load

          val hasAnElement = start != end
          if (!hasAnElement) buff.destroy()
          hasAnElement
        }

        override def next = {
          if (start == end) load

          val res = buff(start)
          start += 1
          res
        }
      }
    })
    val x1684 = x1683.reduceByKey(x1392 _, x49);
    val x1685 = x1684.saveAsTextFile(x56);

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
