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
import ch.epfl.distributed.utils.Helpers.makePartitioner

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
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    @inline
    def x2639(x127: scala.Tuple2[Int, Int], x128: scala.Tuple2[Int, Int]) = {
      val x129 = x127._1;
      val x131 = x128._1;
      val x133 = x129 + x131;
      val x130 = x127._2;
      val x132 = x128._2;
      val x134 = x130 + x132;
      val x135 = (x133, x134);
      x135: scala.Tuple2[Int, Int]
    }
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x2 = x1(0);
    val x7 = x2 + """/lineitem/""";
    val x8 = sc.textFile(x7);
    // x8
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", true, false);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, true, false);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3205 = x8.mapPartitions(it => {
      new Iterator[scala.Tuple2[Long, LineItem]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[Long, LineItem]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x343 = it.next // loop var x341;
            val x859 = x10.split(x343, 16);
            val x887 = x859(14);
            val x2674 = x68.matches(x887);
            val x3203 = if (x2674) {
              val x884 = x859(12);
              val x885 = ch.epfl.distributed.datastruct.Date(x884);
              val x2969 = x5 <= x885;
              val x3201 = if (x2969) {
                val x880 = x859(10);
                val x881 = ch.epfl.distributed.datastruct.Date(x880);
                val x882 = x859(11);
                val x883 = ch.epfl.distributed.datastruct.Date(x882);
                val x2970 = x881 < x883;
                val x3199 = if (x2970) {
                  val x3151 = x883 < x885;
                  val x3162 = if (x3151) {
                    val x3152 = x885 < x91;
                    val x3158 = if (x3152) {
                      val x860 = x859(0);
                      val x861 = x860.toLong;
                      val x862 = x859(1);
                      val x863 = x862.toLong;
                      val x864 = x859(2);
                      val x865 = x864.toLong;
                      val x866 = x859(3);
                      val x867 = x866.toLong;
                      val x868 = x859(4);
                      val x869 = x868.toDouble;
                      val x870 = x859(5);
                      val x871 = x870.toDouble;
                      val x872 = x859(6);
                      val x873 = x872.toDouble;
                      val x874 = x859(7);
                      val x875 = x874.toDouble;
                      val x876 = x859(8);
                      val x877 = x876.charAt(0);
                      val x878 = x859(9);
                      val x879 = x878.charAt(0);
                      val x886 = x859(13);
                      val x888 = x859(15);
                      val x889 = new LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(x861, x863, x865, x867, x869, x871, x873, x875, x877, x879, x881, x883, x885, x886, x887, x888);
                      val x3153 = (x861, x889);
                      buff(i) = x3153 // yield
                      i = i + 1
                      val x3154 = ()
                      x3154
                    } else {
                      val x3156 = () // skip;
                      x3156
                    }
                    x3158
                  } else {
                    val x3160 = () // skip;
                    x3160
                  }
                  x3162
                } else {
                  val x3136 = () // skip;
                  x3136
                }
                x3199
              } else {
                val x3141 = () // skip;
                x3141
              }
              x3201
            } else {
              val x3146 = () // skip;
              x3146
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
    val x44 = x2 + """/orders/""";
    val x45 = sc.textFile(x44);
    // x45
    val x2760 = x45.mapPartitions(it => {
      new Iterator[scala.Tuple2[Long, Order]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[Long, Order]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x325 = it.next // loop var x323;
            val x449 = x10.split(x325, 9);
            val x450 = x449(0);
            val x451 = x450.toLong;
            val x452 = x449(1);
            val x453 = x452.toLong;
            val x454 = x449(2);
            val x455 = x454.charAt(0);
            val x456 = x449(3);
            val x457 = x456.toDouble;
            val x458 = x449(4);
            val x459 = ch.epfl.distributed.datastruct.Date(x458);
            val x460 = x449(5);
            val x461 = x449(6);
            val x462 = x449(7);
            val x463 = x462.toInt;
            val x464 = x449(8);
            val x465 = new Order_0_1_2_3_4_5_6_7_8(x451, x453, x455, x457, x459, x460, x461, x463, x464);
            val x2671 = (x451, x465);
            buff(i) = x2671 // yield
            i = i + 1
            val x2672 = ()
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
    val x3206 = x3205.join(x2760, x106);
    // x3206
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", true, false);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", true, false);
    val x3223 = x3206.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x3208 = it.next // loop var x425;
            val x3209 = x3208._2;
            val x3210 = x3209._2;
            val x3211 = x3210.o_orderpriority;
            val x3212 = x114.matches(x3211);
            val x3214 = if (x3212) {
              true
            } else {
              val x3213 = x116.matches(x3211);
              x3213
            }
            val x3215 = if (x3214) {
              1
            } else {
              0
            }
            val x3216 = 1 - x3215;
            val x3217 = (x3215, x3216);
            val x3218 = x3209._1;
            val x3219 = x3218.l_shipmode;
            val x3220 = (x3219, x3217);
            buff(i) = x3220 // yield
            i = i + 1
            val x3221 = ()
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
    val x3224 = x3223.reduceByKey(x2639 _, 1);
    // x3224
    val x3238 = x3224.mapPartitions(it => {
      new Iterator[java.lang.String] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[java.lang.String](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x3226 = it.next // loop var x436;
            val x3227 = x3226._2;
            val x3228 = x3227._2;
            val x3229 = x3226._1;
            val x3230 = """shipmode """ + x3229;
            val x3231 = x3230 + """: high """;
            val x3232 = x3227._1;
            val x3233 = x3231 + x3232;
            val x3234 = x3233 + """, low """;
            val x3235 = x3234 + x3228;
            buff(i) = x3235 // yield
            i = i + 1
            val x3236 = ()
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
    val x3239 = x3238.saveAsTextFile(x3);

    System.exit(0)
  }
}
// Types that are used in this program
case class LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(override val l_orderkey: Long, override val l_partkey: Long, override val l_suppkey: Long, override val l_linenumber: Long, override val l_quantity: Double, override val l_extendedprice: Double, override val l_discount: Double, override val l_tax: Double, override val l_returnflag: Char, override val l_linestatus: Char, override val l_shipdate: ch.epfl.distributed.datastruct.Date, override val l_commitdate: ch.epfl.distributed.datastruct.Date, override val l_receiptdate: ch.epfl.distributed.datastruct.Date, override val l_shipinstruct: java.lang.String, override val l_shipmode: java.lang.String, override val l_comment: java.lang.String) extends LineItem {
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
case class Order_0_1_2_3_4_5_6_7_8(override val o_orderkey: Long, override val o_custkey: Long, override val o_orderstatus: Char, override val o_totalprice: Double, override val o_orderdate: ch.epfl.distributed.datastruct.Date, override val o_orderpriority: java.lang.String, override val o_clerk: java.lang.String, override val o_shippriority: Int, override val o_comment: java.lang.String) extends Order {
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
  def l_orderkey: Long = throw new RuntimeException("Should not try to access l_orderkey here, internal error")
  def l_partkey: Long = throw new RuntimeException("Should not try to access l_partkey here, internal error")
  def l_suppkey: Long = throw new RuntimeException("Should not try to access l_suppkey here, internal error")
  def l_linenumber: Long = throw new RuntimeException("Should not try to access l_linenumber here, internal error")
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
  def o_orderkey: Long = throw new RuntimeException("Should not try to access o_orderkey here, internal error")
  def o_custkey: Long = throw new RuntimeException("Should not try to access o_custkey here, internal error")
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
