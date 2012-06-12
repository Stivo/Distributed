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

object TpchQueries {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v4.Registrator_TpchQueries")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    @inline
    def x1011(x136: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
      val x138 = x136._2;
      val x142 = x138._2;
      val x137 = x136._1;
      val x139 = """shipmode """ + x137;
      val x140 = x139 + """: high """;
      val x141 = x138._1;
      val x143 = x140 + x141;
      val x144 = x143 + """, low """;
      val x145 = x144 + x142;
      x145: java.lang.String
    }
    @inline
    def x1009(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    @inline
    def x1000(x108: (scala.Tuple2[Int, scala.Tuple2[LineItem, Order]])) = {
      val x110 = x108._2;
      val x112 = x110._2;
      val x113 = x112.o_orderpriority;
      val x114 = x113.startsWith("""1""");
      val x116 = if (x114) {
        true
      } else {
        val x115 = x113.startsWith("""2""");
        x115
      }
      val x117 = if (x116) {
        1
      } else {
        0
      }
      val x118 = 1 - x117;
      val x119 = (x117, x118);
      val x111 = x110._1;
      val x120 = x111.l_shipmode;
      val x121 = (x120, x119);
      x121: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]
    }
    val x2 = x1(0);
    val x8 = x2 + """/lineitem*""";
    val x9 = sc.textFile(x8);
    // x9
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x926(x67: (LineItem)) = {
      val x68 = x67.l_shipmode;
      val x69 = x68 == x6;
      val x71 = if (x69) {
        true
      } else {
        val x70 = x68 == x7;
        x70
      }
      x71: Boolean
    }
    val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, true);
    @inline
    def x917(x10: (java.lang.String)) = {
      val x12 = x11.split(x10, 16);
      val x13 = x12(0);
      val x14 = x13.toInt;
      val x33 = x12(10);
      val x34 = ch.epfl.distributed.datastruct.Date(x33);
      val x35 = x12(11);
      val x36 = ch.epfl.distributed.datastruct.Date(x35);
      val x37 = x12(12);
      val x38 = ch.epfl.distributed.datastruct.Date(x37);
      val x40 = x12(14);
      val x296 = new LineItem_0_10_11_12_14(x14, x34, x36, x38, x40);
      x296: LineItem
    }
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x939(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    @inline
    def x952(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    @inline
    def x965(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x978(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    @inline
    def x916(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x278 = x102.l_shipmode;
      val x279 = new LineItem_14(x278);
      val x280 = (x103, x279);
      x280: scala.Tuple2[Int, LineItem]
    }
    val x1396 = x9.mapPartitions(it => {
      new Iterator[scala.Tuple2[Int, LineItem]] {
        private[this] val buff = new Array[scala.Tuple2[Int, LineItem]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x920 = it.next // loop var x918;
            val x921 = x917(x920);
            val x1039 = x926(x921);
            val x1394 = if (x1039) {
              val x1242 = x939(x921);
              val x1392 = if (x1242) {
                val x1243 = x952(x921);
                val x1390 = if (x1243) {
                  val x1361 = x965(x921);
                  val x1372 = if (x1361) {
                    val x1362 = x978(x921);
                    val x1368 = if (x1362) {
                      val x1363 = x916(x921);
                      buff(i) = x1363 // yield
                      i = i + 1
                      val x1364 = ()
                      x1364
                    } else {
                      val x1366 = () // skip;
                      x1366
                    }
                    x1368
                  } else {
                    val x1370 = () // skip;
                    x1370
                  }
                  x1372
                } else {
                  val x1346 = () // skip;
                  x1346
                }
                x1390
              } else {
                val x1351 = () // skip;
                x1351
              }
              x1392
            } else {
              val x1356 = () // skip;
              x1356
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
    @inline
    def x907(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x206 = x97.o_orderpriority;
      val x207 = new Order_5(x206);
      val x208 = (x98, x207);
      x208: scala.Tuple2[Int, Order]
    }
    @inline
    def x898(x47: (java.lang.String)) = {
      val x48 = x11.split(x47, 9);
      val x49 = x48(0);
      val x50 = x49.toInt;
      val x59 = x48(5);
      val x234 = new Order_0_5(x50, x59);
      x234: Order
    }
    val x1100 = x46.mapPartitions(it => {
      new Iterator[scala.Tuple2[Int, Order]] {
        private[this] val buff = new Array[scala.Tuple2[Int, Order]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x901 = it.next // loop var x899;
            val x902 = x898(x901);
            val x1036 = x907(x902);
            buff(i) = x1036 // yield
            i = i + 1
            val x1037 = ()
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
    val x1397 = x1396.join(x1100);
    // x1397
    val x1403 = x1397.mapPartitions(it => {
      new Iterator[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]] {
        private[this] val buff = new Array[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x1398 = it.next // loop var x1001;
            val x1399 = x1000(x1398);
            buff(i) = x1399 // yield
            i = i + 1
            val x1400 = ()
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
    val x1404 = x1403.reduceByKey(x1009);
    // x1404
    val x1410 = x1404.mapPartitions(it => {
      new Iterator[java.lang.String] {
        private[this] val buff = new Array[java.lang.String](1 << 22)
        private[this] val stopAt = (1 << 22) - (1 << 12);
        private[this] final var start = 0
        private[this] final var end = 0

        @inline
        private[this] final def load = {
          var i = 0
          while (it.hasNext && i < stopAt) {

            val x1405 = it.next // loop var x1012;
            val x1406 = x1011(x1405);
            buff(i) = x1406 // yield
            i = i + 1
            val x1407 = ()
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
    val x1411 = x1410.saveAsTextFile(x3);

    System.exit(0)
  }
}
// Types that are used in this program
case class LineItem_0_10_11_12_14(override val l_orderkey: Int, override val l_shipdate: ch.epfl.distributed.datastruct.Date, override val l_commitdate: ch.epfl.distributed.datastruct.Date, override val l_receiptdate: ch.epfl.distributed.datastruct.Date, override val l_shipmode: java.lang.String) extends LineItem {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("LineItem(")
    sb.append(l_orderkey); sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(",");
    sb.append(l_shipdate); sb.append(",");
    sb.append(l_commitdate); sb.append(",");
    sb.append(l_receiptdate); sb.append(",");
    sb.append(",");
    sb.append(l_shipmode); sb.append(",");
    sb.append(",")
    sb.append(")")
    sb.toString()
  }
}
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
case class Order_0_5(override val o_orderkey: Int, override val o_orderpriority: java.lang.String) extends Order {
  override def toString() = {
    val sb = new StringBuilder()
    sb.append("Order(")
    sb.append(o_orderkey); sb.append(",");
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
    kryo.register(classOf[LineItem_0_10_11_12_14])
    kryo.register(classOf[LineItem_14])
    kryo.register(classOf[Order])
    kryo.register(classOf[Order_0_5])
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
