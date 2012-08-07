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

object TpchQueries {
  // field reduction: true
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v2.Registrator_TpchQueries")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", false, false);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", false, false);
    @inline
    def x849(x108: (scala.Tuple2[Long, scala.Tuple2[LineItem, Order]])) = {
      val x110 = x108._2;
      val x112 = x110._2;
      val x113 = x112.o_orderpriority;
      val x115 = x114.matches(x113);
      val x118 = if (x115) {
        true
      } else {
        val x117 = x116.matches(x113);
        x117
      }
      val x119 = if (x118) {
        1
      } else {
        0
      }
      val x120 = 1 - x119;
      val x121 = (x119, x120);
      val x111 = x110._1;
      val x122 = x111.l_shipmode;
      val x123 = (x122, x121);
      x123: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]
    }
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x2 = x1(0);
    val x44 = x2 + """/orders/""";
    val x45 = sc.textFile(x44);
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    @inline
    def x850(x46: (java.lang.String)) = {
      val x47 = x10.split(x46, 9);
      val x48 = x47(0);
      val x49 = x48.toLong;
      val x58 = x47(5);
      val x236 = new Order_0_5(x49, x58);
      x236: Order
    }
    val x851 = x45.map(x850);
    @inline
    def x852(x95: (Order)) = {
      val x96 = x95.o_orderkey;
      val x208 = x95.o_orderpriority;
      val x209 = new Order_5(x208);
      val x210 = (x96, x209);
      x210: scala.Tuple2[Long, Order]
    }
    val x853 = x851.map(x852);
    @inline
    def x854(x100: (LineItem)) = {
      val x101 = x100.l_orderkey;
      val x280 = x100.l_shipmode;
      val x281 = new LineItem_14(x280);
      val x282 = (x101, x281);
      x282: scala.Tuple2[Long, LineItem]
    }
    val x7 = x2 + """/lineitem/""";
    val x8 = sc.textFile(x7);
    @inline
    def x778(x9: (java.lang.String)) = {
      val x11 = x10.split(x9, 16);
      val x12 = x11(0);
      val x13 = x12.toLong;
      val x32 = x11(10);
      val x33 = ch.epfl.distributed.datastruct.Date(x32);
      val x34 = x11(11);
      val x35 = ch.epfl.distributed.datastruct.Date(x34);
      val x36 = x11(12);
      val x37 = ch.epfl.distributed.datastruct.Date(x36);
      val x39 = x11(14);
      val x298 = new LineItem_0_10_11_12_14(x13, x33, x35, x37, x39);
      x298: LineItem
    }
    val x779 = x8.map(x778);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, false, false);
    @inline
    def x855(x66: (LineItem)) = {
      val x67 = x66.l_shipmode;
      val x69 = x68.matches(x67);
      x69: Boolean
    }
    val x856 = x779.filter(x855);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x857(x72: (LineItem)) = {
      val x73 = x72.l_receiptdate;
      val x74 = x5 <= x73;
      x74: Boolean
    }
    val x858 = x856.filter(x857);
    @inline
    def x859(x77: (LineItem)) = {
      val x78 = x77.l_shipdate;
      val x79 = x77.l_commitdate;
      val x80 = x78 < x79;
      x80: Boolean
    }
    val x860 = x858.filter(x859);
    @inline
    def x861(x83: (LineItem)) = {
      val x84 = x83.l_commitdate;
      val x85 = x83.l_receiptdate;
      val x86 = x84 < x85;
      x86: Boolean
    }
    val x862 = x860.filter(x861);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x863(x89: (LineItem)) = {
      val x90 = x89.l_receiptdate;
      val x92 = x90 < x91;
      x92: Boolean
    }
    val x864 = x862.filter(x863);
    val x865 = x864.map(x854);
    val x866 = x865.join(x853, x106);
    val x867 = x866.map(x849);
    @inline
    def x868(x127: scala.Tuple2[Int, Int], x128: scala.Tuple2[Int, Int]) = {
      val x129 = x127._1;
      val x131 = x128._1;
      val x133 = x129 + x131;
      val x130 = x127._2;
      val x132 = x128._2;
      val x134 = x130 + x132;
      val x135 = (x133, x134);
      x135: scala.Tuple2[Int, Int]
    }
    val x869 = x867.reduceByKey(x868 _, 1);
    @inline
    def x895(x138: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
      val x140 = x138._2;
      val x144 = x140._2;
      val x139 = x138._1;
      val x141 = """shipmode """ + x139;
      val x142 = x141 + """: high """;
      val x143 = x140._1;
      val x145 = x142 + x143;
      val x146 = x145 + """, low """;
      val x147 = x146 + x144;
      x147: java.lang.String
    }
    val x896 = x869.map(x895);
    val x897 = x896.saveAsTextFile(x3);

    System.exit(0)
  }
}
// Types that are used in this program
case class LineItem_0_10_11_12_14(override val l_orderkey: Long, override val l_shipdate: ch.epfl.distributed.datastruct.Date, override val l_commitdate: ch.epfl.distributed.datastruct.Date, override val l_receiptdate: ch.epfl.distributed.datastruct.Date, override val l_shipmode: java.lang.String) extends LineItem {
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
case class Order_0_5(override val o_orderkey: Long, override val o_orderpriority: java.lang.String) extends Order {
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
