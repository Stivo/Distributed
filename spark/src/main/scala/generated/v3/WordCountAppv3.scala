/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated.v3;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo

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
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v3.Registrator_WordCountApp")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")

    val sc = new SparkContext(sparkInputArgs(0), "WordCountApp")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x59 = x1(1);
    @inline
    def x1376(x54: Int, x55: Int) = {
      val x56 = x54 + x55;
      x56: Int
    }
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    // x3
    val x5 = new ch.epfl.distributed.datastruct.RegexFrontend("""	""", false, true);
    val x23 = new ch.epfl.distributed.datastruct.RegexFrontend("""\[\[.*?\]\]""", false, true);
    val x28 = new ch.epfl.distributed.datastruct.RegexFrontend("""(\\[ntT]|\.)\s*(thumb|left|right)*""", false, true);
    val x33 = new ch.epfl.distributed.datastruct.RegexFrontend("""[^a-zA-Z0-9']+""", false, true);
    val x44 = new ch.epfl.distributed.datastruct.RegexFrontend("""(thumb|left|right|\d+px){2,}""", false, true);
    val x1667 = x3.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, Int]] {
        private[this] val buff = new Array[scala.Tuple2[java.lang.String, Int]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x220 = it.next // loop var x218;
            val x312 = x5.split(x220, 5);
            val x313 = x312(4);
            val x1391 = """\n""" + x313;
            val x1570 = x23.replaceAll(x1391, """ """);
            val x1571 = x28.replaceAll(x1570, """ """);
            val x1643 = x33.split(x1571, 0);
            val x1644 = x1643.toSeq;
            // x1644
            {
              val it = x1644.iterator
              while (it.hasNext) { // flatMap
                val x1646 = it.next // loop var x259;
                val x1647 = x1646.length;
                val x1648 = x1647 > 1;
                val x1660 = if (x1648) {
                  val x1649 = x44.matches(x1646);
                  val x1650 = !x1649;
                  val x1656 = if (x1650) {
                    val x1651 = (x1646, 1);
                    buff(i) = x1651 // yield
                    i = i + 1
                    val x1652 = ()
                    x1652
                  } else {
                    val x1654 = () // skip;
                    x1654
                  }
                  x1656
                } else {
                  val x1658 = () // skip;
                  x1658
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
    val x1668 = x1667.reduceByKey(x1376);
    val x1669 = x1668.saveAsTextFile(x59);

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
