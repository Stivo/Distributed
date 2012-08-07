/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated.v4;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo
import ch.epfl.distributed.utils.Helpers.makePartitioner

object TpchQueries {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v4.Registrator_TpchQueries")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    @inline
    def x3236(x127: scala.Tuple2[Int, Int], x128: scala.Tuple2[Int, Int]) = {
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
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", true, true);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, true, true);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3816 = x8.mapPartitions(it => {
      new Iterator[scala.Tuple2[Long, LineItem]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[Long, LineItem]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x922 = it.next // loop var x920;
            val x1430 = x10.split(x922, 16);
            val x1439 = x1430(14);
            val x3272 = x68.matches(x1439);
            val x3814 = if (x3272) {
              val x1437 = x1430(12);
              val x1438 = ch.epfl.distributed.datastruct.Date(x1437);
              val x3575 = x5 <= x1438;
              val x3812 = if (x3575) {
                val x1433 = x1430(10);
                val x1434 = ch.epfl.distributed.datastruct.Date(x1433);
                val x1435 = x1430(11);
                val x1436 = ch.epfl.distributed.datastruct.Date(x1435);
                val x3576 = x1434 < x1436;
                val x3810 = if (x3576) {
                  val x3761 = x1436 < x1438;
                  val x3773 = if (x3761) {
                    val x3762 = x1438 < x91;
                    val x3769 = if (x3762) {
                      val x1431 = x1430(0);
                      val x1432 = x1431.toLong;
                      val x3763 = new LineItem_14(x1439);
                      val x3764 = (x1432, x3763);
                      buff(i) = x3764 // yield
                      i = i + 1
                      val x3765 = ()
                      x3765
                    } else {
                      val x3767 = () // skip;
                      x3767
                    }
                    x3769
                  } else {
                    val x3771 = () // skip;
                    x3771
                  }
                  x3773
                } else {
                  val x3746 = () // skip;
                  x3746
                }
                x3810
              } else {
                val x3751 = () // skip;
                x3751
              }
              x3812
            } else {
              val x3756 = () // skip;
              x3756
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
    val x3360 = x45.mapPartitions(it => {
      new Iterator[scala.Tuple2[Long, Order]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[Long, Order]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x903 = it.next // loop var x901;
            val x1026 = x10.split(x903, 9);
            val x1027 = x1026(0);
            val x1028 = x1027.toLong;
            val x1029 = x1026(5);
            val x3268 = new Order_5(x1029);
            val x3269 = (x1028, x3268);
            buff(i) = x3269 // yield
            i = i + 1
            val x3270 = ()
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
    val x3817 = x3816.join(x3360, x106);
    // x3817
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", true, true);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", true, true);
    val x3834 = x3817.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x3819 = it.next // loop var x1002;
            val x3820 = x3819._2;
            val x3821 = x3820._2;
            val x3822 = x3821.o_orderpriority;
            val x3823 = x114.matches(x3822);
            val x3825 = if (x3823) {
              true
            } else {
              val x3824 = x116.matches(x3822);
              x3824
            }
            val x3826 = if (x3825) {
              1
            } else {
              0
            }
            val x3827 = 1 - x3826;
            val x3828 = (x3826, x3827);
            val x3829 = x3820._1;
            val x3830 = x3829.l_shipmode;
            val x3831 = (x3830, x3828);
            buff(i) = x3831 // yield
            i = i + 1
            val x3832 = ()
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
    val x3835 = x3834.reduceByKey(x3236 _, 1);
    // x3835
    val x3849 = x3835.mapPartitions(it => {
      new Iterator[java.lang.String] {
        private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[java.lang.String](1 << 10)
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          buff.clear()
          while (i == 0 && it.hasNext) {

            val x3837 = it.next // loop var x1013;
            val x3838 = x3837._2;
            val x3839 = x3838._2;
            val x3840 = x3837._1;
            val x3841 = """shipmode """ + x3840;
            val x3842 = x3841 + """: high """;
            val x3843 = x3838._1;
            val x3844 = x3842 + x3843;
            val x3845 = x3844 + """, low """;
            val x3846 = x3845 + x3839;
            buff(i) = x3846 // yield
            i = i + 1
            val x3847 = ()
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
    val x3850 = x3849.saveAsTextFile(x3);

    System.exit(0)
  }
}
// Types that are used in this program
case class LineItem_14(override val l_shipmode: java.lang.String) extends LineItem {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("LineItem(")
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(l_shipmode); sb.append(",");
    sb.append(",")
    sb.append(")")
    sb.toString()
  }
}
case class Order_5(override val o_orderpriority: java.lang.String) extends Order {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("Order(")
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(o_orderpriority); sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",")
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
    kryo.register(classOf[LineItem_14])
    kryo.register(classOf[Order])
    kryo.register(classOf[Order_5])
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
