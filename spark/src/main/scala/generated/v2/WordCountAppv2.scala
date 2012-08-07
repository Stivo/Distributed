/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated.v2;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo
import ch.epfl.distributed.utils.Helpers.makePartitioner

object WordCountApp {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v2.Registrator_WordCountApp")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "WordCountApp")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x61 = x1(1);
    val x53 = x1(2);
    val x54 = x53.toInt;
    @inline
    def x1397(x56: Int, x57: Int) = {
      val x58 = x56 + x57;
      x58: Int
    }
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    // x3
    val x5 = new ch.epfl.distributed.datastruct.RegexFrontend("""	""", false, false);
    val x23 = new ch.epfl.distributed.datastruct.RegexFrontend("""\[\[.*?\]\]""", false, false);
    val x28 = new ch.epfl.distributed.datastruct.RegexFrontend("""(\\[ntT]|\.)\s*(thumb|left|right)*""", false, false);
    val x33 = new ch.epfl.distributed.datastruct.RegexFrontend("""[^a-zA-Z0-9']+""", false, false);
    val x44 = new ch.epfl.distributed.datastruct.RegexFrontend("""(thumb|left|right|\d+px){2,}""", false, false);
    val x1688 = x3.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, Int]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[java.lang.String, Int]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x241 = it.next // loop var x239;
            val x333 = x5.split(x241, 5);
            val x334 = x333(4);
            val x1412 = """\n""" + x334;
            val x1591 = x23.replaceAll(x1412, """ """);
            val x1592 = x28.replaceAll(x1591, """ """);
            val x1664 = x33.split(x1592, 0);
            val x1665 = x1664.toSeq;
            // x1665
            {
              val it = x1665.iterator
              while (it.hasNext) { // flatMap
                val x1667 = it.next // loop var x280;
                val x1668 = x1667.length;
                val x1669 = x1668 > 1;
                val x1681 = if (x1669) {
                  val x1670 = x44.matches(x1667);
                  val x1671 = !x1670;
                  val x1677 = if (x1671) {
                    val x1672 = (x1667, 1);
                    buff(i) = x1672 // yield
                    i = i + 1
                    val x1673 = ()
                    x1673
                  } else {
                    val x1675 = () // skip;
                    x1675
                  }
                  x1677
                } else {
                  val x1679 = () // skip;
                  x1679
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
    val x1689 = x1688.reduceByKey(x1397 _, x54);
    val x1690 = x1689.saveAsTextFile(x61);

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
