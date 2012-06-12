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

    val sc = new SparkContext(sparkInputArgs(0), "TpchQueries")

    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x3 = x1(1);
    val x2 = x1(0);
    val x45 = x2 + """/orders.tbl""";
    val x46 = sc.textFile(x45);
    val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    @inline
    def x848(x47: (java.lang.String)) = {
      val x48 = x11.split(x47, 9);
      val x49 = x48(0);
      val x50 = x49.toInt;
      val x59 = x48(5);
      val x234 = new Order_0_5(x50, x59);
      x234: Order
    }
    val x849 = x46.map(x848);
    @inline
    def x850(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x206 = x97.o_orderpriority;
      val x207 = new Order_5(x206);
      val x208 = (x98, x207);
      x208: scala.Tuple2[Int, Order]
    }
    val x851 = x849.map(x850);
    @inline
    def x852(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x278 = x102.l_shipmode;
      val x279 = new LineItem_14(x278);
      val x280 = (x103, x279);
      x280: scala.Tuple2[Int, LineItem]
    }
    val x8 = x2 + """/lineitem*""";
    val x9 = sc.textFile(x8);
    @inline
    def x776(x10: (java.lang.String)) = {
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
    val x777 = x9.map(x776);
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x853(x67: (LineItem)) = {
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
    val x854 = x777.filter(x853);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x855(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    val x856 = x854.filter(x855);
    @inline
    def x857(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    val x858 = x856.filter(x857);
    @inline
    def x859(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    val x860 = x858.filter(x859);
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x861(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    val x862 = x860.filter(x861);
    val x863 = x862.map(x852);
    val x864 = x863.join(x851);
    @inline
    def x872(x108: (scala.Tuple2[Int, scala.Tuple2[LineItem, Order]])) = {
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
    val x891 = x864.map(x872);
    @inline
    def x892(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x893 = x891.reduceByKey(x892);
    @inline
    def x894(x136: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
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
    val x895 = x893.map(x894);
    val x896 = x895.saveAsTextFile(x3);

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
