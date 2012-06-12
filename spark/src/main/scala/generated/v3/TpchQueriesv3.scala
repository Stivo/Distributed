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

object TpchQueries {
  // field reduction: false
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v3.Registrator_TpchQueries")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    @inline
    def x2669(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x2 = x1(0);
    val x8 = x2 + """/lineitem*""";
    val x9 = sc.textFile(x8);
    // x9
    val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    val x6 = x1(3);
    val x7 = x1(4);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3237 = x9.mapPartitions(it => {
      new Iterator[scala.Tuple2[Int, LineItem]] {
        private[this] val buff = new Array[scala.Tuple2[Int, LineItem]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x341 = it.next // loop var x339;
            val x857 = x11.split(x341, 16);
            val x885 = x857(14);
            val x2704 = x885 == x6;
            val x2706 = if (x2704) {
              true
            } else {
              val x2705 = x885 == x7;
              x2705
            }
            val x3235 = if (x2706) {
              val x882 = x857(12);
              val x883 = ch.epfl.distributed.datastruct.Date(x882);
              val x3001 = x5 <= x883;
              val x3233 = if (x3001) {
                val x878 = x857(10);
                val x879 = ch.epfl.distributed.datastruct.Date(x878);
                val x880 = x857(11);
                val x881 = ch.epfl.distributed.datastruct.Date(x880);
                val x3002 = x879 < x881;
                val x3231 = if (x3002) {
                  val x3183 = x881 < x883;
                  val x3194 = if (x3183) {
                    val x3184 = x883 < x93;
                    val x3190 = if (x3184) {
                      val x858 = x857(0);
                      val x859 = x858.toInt;
                      val x860 = x857(1);
                      val x861 = x860.toInt;
                      val x862 = x857(2);
                      val x863 = x862.toInt;
                      val x864 = x857(3);
                      val x865 = x864.toInt;
                      val x866 = x857(4);
                      val x867 = x866.toDouble;
                      val x868 = x857(5);
                      val x869 = x868.toDouble;
                      val x870 = x857(6);
                      val x871 = x870.toDouble;
                      val x872 = x857(7);
                      val x873 = x872.toDouble;
                      val x874 = x857(8);
                      val x875 = x874.charAt(0);
                      val x876 = x857(9);
                      val x877 = x876.charAt(0);
                      val x884 = x857(13);
                      val x886 = x857(15);
                      val x887 = new LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(x859, x861, x863, x865, x867, x869, x871, x873, x875, x877, x879, x881, x883, x884, x885, x886);
                      val x3185 = (x859, x887);
                      buff(i) = x3185 // yield
                      i = i + 1
                      val x3186 = ()
                      x3186
                    } else {
                      val x3188 = () // skip;
                      x3188
                    }
                    x3190
                  } else {
                    val x3192 = () // skip;
                    x3192
                  }
                  x3194
                } else {
                  val x3168 = () // skip;
                  x3168
                }
                x3231
              } else {
                val x3173 = () // skip;
                x3173
              }
              x3233
            } else {
              val x3178 = () // skip;
              x3178
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
    val x45 = x2 + """/orders.tbl""";
    val x46 = sc.textFile(x45);
    // x46
    val x2792 = x46.mapPartitions(it => {
      new Iterator[scala.Tuple2[Int, Order]] {
        private[this] val buff = new Array[scala.Tuple2[Int, Order]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x323 = it.next // loop var x321;
            val x447 = x11.split(x323, 9);
            val x448 = x447(0);
            val x449 = x448.toInt;
            val x450 = x447(1);
            val x451 = x450.toInt;
            val x452 = x447(2);
            val x453 = x452.charAt(0);
            val x454 = x447(3);
            val x455 = x454.toDouble;
            val x456 = x447(4);
            val x457 = ch.epfl.distributed.datastruct.Date(x456);
            val x458 = x447(5);
            val x459 = x447(6);
            val x460 = x447(7);
            val x461 = x460.toInt;
            val x462 = x447(8);
            val x463 = new Order_0_1_2_3_4_5_6_7_8(x449, x451, x453, x455, x457, x458, x459, x461, x462);
            val x2701 = (x449, x463);
            buff(i) = x2701 // yield
            i = i + 1
            val x2702 = ()
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
    val x3238 = x3237.join(x2792);
    // x3238
    val x3255 = x3238.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]] {
        private[this] val buff = new Array[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x3240 = it.next // loop var x423;
            val x3241 = x3240._2;
            val x3242 = x3241._2;
            val x3243 = x3242.o_orderpriority;
            val x3244 = x3243.startsWith("""1""");
            val x3246 = if (x3244) {
              true
            } else {
              val x3245 = x3243.startsWith("""2""");
              x3245
            }
            val x3247 = if (x3246) {
              1
            } else {
              0
            }
            val x3248 = 1 - x3247;
            val x3249 = (x3247, x3248);
            val x3250 = x3241._1;
            val x3251 = x3250.l_shipmode;
            val x3252 = (x3251, x3249);
            buff(i) = x3252 // yield
            i = i + 1
            val x3253 = ()
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
    val x3256 = x3255.reduceByKey(x2669);
    // x3256
    val x3270 = x3256.mapPartitions(it => {
      new Iterator[java.lang.String] {
        private[this] val buff = new Array[java.lang.String](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x3258 = it.next // loop var x434;
            val x3259 = x3258._2;
            val x3260 = x3259._2;
            val x3261 = x3258._1;
            val x3262 = """shipmode """ + x3261;
            val x3263 = x3262 + """: high """;
            val x3264 = x3259._1;
            val x3265 = x3263 + x3264;
            val x3266 = x3265 + """, low """;
            val x3267 = x3266 + x3260;
            buff(i) = x3267 // yield
            i = i + 1
            val x3268 = ()
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
    val x3271 = x3270.saveAsTextFile(x3);

    System.exit(0)
  }
}
// Types that are used in this program
case class LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(override val l_orderkey: Int, override val l_partkey: Int, override val l_suppkey: Int, override val l_linenumber: Int, override val l_quantity: Double, override val l_extendedprice: Double, override val l_discount: Double, override val l_tax: Double, override val l_returnflag: Char, override val l_linestatus: Char, override val l_shipdate: ch.epfl.distributed.datastruct.Date, override val l_commitdate: ch.epfl.distributed.datastruct.Date, override val l_receiptdate: ch.epfl.distributed.datastruct.Date, override val l_shipinstruct: java.lang.String, override val l_shipmode: java.lang.String, override val l_comment: java.lang.String) extends LineItem {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("LineItem(")
    sb.append(l_orderkey); sb.append(",");
    sb.append(l_partkey); sb.append(",");
    sb.append(l_suppkey); sb.append(",");
    sb.append(l_linenumber); sb.append(",");
    sb.append(l_quantity); sb.append(",");
    sb.append(l_extendedprice); sb.append(",");
    sb.append(l_discount); sb.append(",");
    sb.append(l_tax); sb.append(",");
    sb.append(l_returnflag); sb.append(",");
    sb.append(l_linestatus); sb.append(",");
    sb.append(l_shipdate); sb.append(",");
    sb.append(l_commitdate); sb.append(",");
    sb.append(l_receiptdate); sb.append(",");
    sb.append(l_shipinstruct); sb.append(",");
    sb.append(l_shipmode); sb.append(",");
    sb.append(l_comment); sb.append(",")
    sb.append(")")
    sb.toString()
  }
}
case class Order_0_1_2_3_4_5_6_7_8(override val o_orderkey: Int, override val o_custkey: Int, override val o_orderstatus: Char, override val o_totalprice: Double, override val o_orderdate: ch.epfl.distributed.datastruct.Date, override val o_orderpriority: java.lang.String, override val o_clerk: java.lang.String, override val o_shippriority: Int, override val o_comment: java.lang.String) extends Order {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("Order(")
    sb.append(o_orderkey); sb.append(",");
    sb.append(o_custkey); sb.append(",");
    sb.append(o_orderstatus); sb.append(",");
    sb.append(o_totalprice); sb.append(",");
    sb.append(o_orderdate); sb.append(",");
    sb.append(o_orderpriority); sb.append(",");
    sb.append(o_clerk); sb.append(",");
    sb.append(o_shippriority); sb.append(",");
    sb.append(o_comment); sb.append(",")
    sb.append(")")
    sb.toString()
  }
}
trait LineItem extends Serializable {
  def l_orderkey: Int = throw new RuntimeException("Should not try to access l_orderkey here, internal error")
  def l_partkey: Int = throw new RuntimeException("Should not try to access l_partkey here, internal error")
  def l_suppkey: Int = throw new RuntimeException("Should not try to access l_suppkey here, internal error")
  def l_linenumber: Int = throw new RuntimeException("Should not try to access l_linenumber here, internal error")
  def l_quantity: Double = throw new RuntimeException("Should not try to access l_quantity here, internal error")
  def l_extendedprice: Double = throw new RuntimeException("Should not try to access l_extendedprice here, internal error")
  def l_discount: Double = throw new RuntimeException("Should not try to access l_discount here, internal error")
  def l_tax: Double = throw new RuntimeException("Should not try to access l_tax here, internal error")
  def l_returnflag: Char = throw new RuntimeException("Should not try to access l_returnflag here, internal error")
  def l_linestatus: Char = throw new RuntimeException("Should not try to access l_linestatus here, internal error")
  def l_shipdate: ch.epfl.distributed.datastruct.Date = throw new RuntimeException("Should not try to access l_shipdate here, internal error")
  def l_commitdate: ch.epfl.distributed.datastruct.Date = throw new RuntimeException("Should not try to access l_commitdate here, internal error")
  def l_receiptdate: ch.epfl.distributed.datastruct.Date = throw new RuntimeException("Should not try to access l_receiptdate here, internal error")
  def l_shipinstruct: java.lang.String = throw new RuntimeException("Should not try to access l_shipinstruct here, internal error")
  def l_shipmode: java.lang.String = throw new RuntimeException("Should not try to access l_shipmode here, internal error")
  def l_comment: java.lang.String = throw new RuntimeException("Should not try to access l_comment here, internal error")
}
trait Order extends Serializable {
  def o_orderkey: Int = throw new RuntimeException("Should not try to access o_orderkey here, internal error")
  def o_custkey: Int = throw new RuntimeException("Should not try to access o_custkey here, internal error")
  def o_orderstatus: Char = throw new RuntimeException("Should not try to access o_orderstatus here, internal error")
  def o_totalprice: Double = throw new RuntimeException("Should not try to access o_totalprice here, internal error")
  def o_orderdate: ch.epfl.distributed.datastruct.Date = throw new RuntimeException("Should not try to access o_orderdate here, internal error")
  def o_orderpriority: java.lang.String = throw new RuntimeException("Should not try to access o_orderpriority here, internal error")
  def o_clerk: java.lang.String = throw new RuntimeException("Should not try to access o_clerk here, internal error")
  def o_shippriority: Int = throw new RuntimeException("Should not try to access o_shippriority here, internal error")
  def o_comment: java.lang.String = throw new RuntimeException("Should not try to access o_comment here, internal error")
}
class Registrator_TpchQueries extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[LineItem])
    kryo.register(classOf[LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15])
    kryo.register(classOf[Order])
    kryo.register(classOf[Order_0_1_2_3_4_5_6_7_8])
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
