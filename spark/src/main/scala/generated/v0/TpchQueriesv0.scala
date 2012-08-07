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

object TpchQueries {
  // field reduction: false
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.v0.Registrator_TpchQueries")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.cache.class", "spark.DiskSpillingCache")

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x2 = x1(0);
    val x44 = x2 + """/orders/""";
    val x45 = sc.textFile(x44);
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    @inline
    def x152(x46: (java.lang.String)) = {
      val x47 = x10.split(x46, 9);
      val x48 = x47(0);
      val x49 = x48.toLong;
      val x50 = x47(1);
      val x51 = x50.toLong;
      val x52 = x47(2);
      val x53 = x52.charAt(0);
      val x54 = x47(3);
      val x55 = x54.toDouble;
      val x56 = x47(4);
      val x57 = ch.epfl.distributed.datastruct.Date(x56);
      val x58 = x47(5);
      val x59 = x47(6);
      val x60 = x47(7);
      val x61 = x60.toInt;
      val x62 = x47(8);
      val x63 = new Order_0_1_2_3_4_5_6_7_8(x49, x51, x53, x55, x57, x58, x59, x61, x62);
      x63: Order
    }
    val x153 = x45.map(x152);
    @inline
    def x154(x95: (Order)) = {
      val x96 = x95.o_orderkey;
      val x97 = (x96, x95);
      x97: scala.Tuple2[Long, Order]
    }
    val x155 = x153.map(x154);
    val x7 = x2 + """/lineitem/""";
    val x8 = sc.textFile(x7);
    @inline
    def x156(x9: (java.lang.String)) = {
      val x11 = x10.split(x9, 16);
      val x12 = x11(0);
      val x13 = x12.toLong;
      val x14 = x11(1);
      val x15 = x14.toLong;
      val x16 = x11(2);
      val x17 = x16.toLong;
      val x18 = x11(3);
      val x19 = x18.toLong;
      val x20 = x11(4);
      val x21 = x20.toDouble;
      val x22 = x11(5);
      val x23 = x22.toDouble;
      val x24 = x11(6);
      val x25 = x24.toDouble;
      val x26 = x11(7);
      val x27 = x26.toDouble;
      val x28 = x11(8);
      val x29 = x28.charAt(0);
      val x30 = x11(9);
      val x31 = x30.charAt(0);
      val x32 = x11(10);
      val x33 = ch.epfl.distributed.datastruct.Date(x32);
      val x34 = x11(11);
      val x35 = ch.epfl.distributed.datastruct.Date(x34);
      val x36 = x11(12);
      val x37 = ch.epfl.distributed.datastruct.Date(x36);
      val x38 = x11(13);
      val x39 = x11(14);
      val x40 = x11(15);
      val x41 = new LineItem_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(x13, x15, x17, x19, x21, x23, x25, x27, x29, x31, x33, x35, x37, x38, x39, x40);
      x41: LineItem
    }
    val x157 = x8.map(x156);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, false, false);
    @inline
    def x158(x66: (LineItem)) = {
      val x67 = x66.l_shipmode;
      val x69 = x68.matches(x67);
      x69: Boolean
    }
    val x159 = x157.filter(x158);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x160(x72: (LineItem)) = {
      val x73 = x72.l_receiptdate;
      val x74 = x5 <= x73;
      x74: Boolean
    }
    val x161 = x159.filter(x160);
    @inline
    def x162(x77: (LineItem)) = {
      val x78 = x77.l_shipdate;
      val x79 = x77.l_commitdate;
      val x80 = x78 < x79;
      x80: Boolean
    }
    val x163 = x161.filter(x162);
    @inline
    def x164(x83: (LineItem)) = {
      val x84 = x83.l_commitdate;
      val x85 = x83.l_receiptdate;
      val x86 = x84 < x85;
      x86: Boolean
    }
    val x165 = x163.filter(x164);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x166(x89: (LineItem)) = {
      val x90 = x89.l_receiptdate;
      val x92 = x90 < x91;
      x92: Boolean
    }
    val x167 = x165.filter(x166);
    @inline
    def x168(x100: (LineItem)) = {
      val x101 = x100.l_orderkey;
      val x102 = (x101, x100);
      x102: scala.Tuple2[Long, LineItem]
    }
    val x169 = x167.map(x168);
    val x170 = x169.join(x155, x106);
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", false, false);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", false, false);
    @inline
    def x171(x108: (scala.Tuple2[Long, scala.Tuple2[LineItem, Order]])) = {
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
    val x172 = x170.map(x171);
    @inline
    def x174(x127: scala.Tuple2[Int, Int], x128: scala.Tuple2[Int, Int]) = {
      val x129 = x127._1;
      val x131 = x128._1;
      val x133 = x129 + x131;
      val x130 = x127._2;
      val x132 = x128._2;
      val x134 = x130 + x132;
      val x135 = (x133, x134);
      x135: scala.Tuple2[Int, Int]
    }
    val x175 = x172.reduceByKey(x174 _, 1);
    @inline
    def x176(x138: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
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
    val x177 = x175.map(x176);
    val x178 = x177.saveAsTextFile(x3);

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
