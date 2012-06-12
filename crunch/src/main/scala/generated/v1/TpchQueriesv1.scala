/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v1;

import java.io.DataInput
import java.io.DataOutput
import java.io.Serializable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import com.cloudera.crunch.`type`.writable.Writables
//import com.cloudera.crunch.types.writable.Writables
import com.cloudera.crunch.impl.mr.MRPipeline
import com.cloudera.crunch.DoFn
import com.cloudera.crunch.Emitter
import com.cloudera.crunch.{ Pair => CPair }

import ch.epfl.distributed.utils.JoinHelper._
import ch.epfl.distributed.utils._

import com.cloudera.crunch._

object TpchQueries {
  def main(args: Array[String]) {
    val newArgs = (List("asdf") ++ args.toList).toArray
    ToolRunner.run(new Configuration(), new TpchQueries(), newArgs);
  }
}

class TpchQueries extends Configured with Tool with Serializable {
  // field reduction: false
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[TpchQueries], getConf());

    val x1 = args.drop(1);
    val x3 = x1(1);
    val x2 = x1(0);
    val x45 = x2 + """/orders.tbl""";
    val x46 = pipeline.readTextFile(x45);
    val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, true);
    @inline
    def x65(x47: (java.lang.String)) = {
      val x48 = x11.split(x47, 9);
      val x49 = x48(0);
      val x50 = x49.toInt;
      val x51 = x48(1);
      val x52 = x51.toInt;
      val x53 = x48(2);
      val x54 = x53.charAt(0);
      val x55 = x48(3);
      val x56 = x55.toDouble;
      val x57 = x48(4);
      val x58 = ch.epfl.distributed.datastruct.Date(x57);
      val x59 = x48(5);
      val x60 = x48(6);
      val x61 = x48(7);
      val x62 = x61.toInt;
      val x63 = x48(8);
      val x64 = new Order(o_orderkey = x50, o_custkey = x52, o_orderstatus = x54, o_totalprice = x56, o_orderdate = x58, o_orderpriority = x59, o_clerk = x60, o_shippriority = x62, o_comment = x63);
      x64.__bitset = 511
      x64: Order
    }
    val x66 = x46.parallelDo(new DoFn[java.lang.String, Order] {
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {
        emitter.emit(x65(input))
      }
    }, Writables.records(classOf[Order]));
    @inline
    def x100(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x99 = CPair.of(x98.asInstanceOf[java.lang.Integer], x97);
      x99: CPair[java.lang.Integer, Order]
    }
    val x101 = x66.parallelDo(new DoFn[Order, CPair[java.lang.Integer, Order]] {
      def process(input: Order, emitter: Emitter[CPair[java.lang.Integer, Order]]): Unit = {
        emitter.emit(x100(input))
      }
    }, Writables.tableOf(Writables.ints(), Writables.records(classOf[Order])));
    val x8 = x2 + """lineitem/*""";
    val x9 = pipeline.readTextFile(x8);
    @inline
    def x43(x10: (java.lang.String)) = {
      val x12 = x11.split(x10, 16);
      val x13 = x12(0);
      val x14 = x13.toInt;
      val x15 = x12(1);
      val x16 = x15.toInt;
      val x17 = x12(2);
      val x18 = x17.toInt;
      val x19 = x12(3);
      val x20 = x19.toInt;
      val x21 = x12(4);
      val x22 = x21.toDouble;
      val x23 = x12(5);
      val x24 = x23.toDouble;
      val x25 = x12(6);
      val x26 = x25.toDouble;
      val x27 = x12(7);
      val x28 = x27.toDouble;
      val x29 = x12(8);
      val x30 = x29.charAt(0);
      val x31 = x12(9);
      val x32 = x31.charAt(0);
      val x33 = x12(10);
      val x34 = ch.epfl.distributed.datastruct.Date(x33);
      val x35 = x12(11);
      val x36 = ch.epfl.distributed.datastruct.Date(x35);
      val x37 = x12(12);
      val x38 = ch.epfl.distributed.datastruct.Date(x37);
      val x39 = x12(13);
      val x40 = x12(14);
      val x41 = x12(15);
      val x42 = new LineItem(l_orderkey = x14, l_partkey = x16, l_suppkey = x18, l_linenumber = x20, l_quantity = x22, l_extendedprice = x24, l_discount = x26, l_tax = x28, l_returnflag = x30, l_linestatus = x32, l_shipdate = x34, l_commitdate = x36, l_receiptdate = x38, l_shipinstruct = x39, l_shipmode = x40, l_comment = x41);
      x42.__bitset = 65535
      x42: LineItem
    }
    val x44 = x9.parallelDo(new DoFn[java.lang.String, LineItem] {
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {
        emitter.emit(x43(input))
      }
    }, Writables.records(classOf[LineItem]));
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x72(x67: (LineItem)) = {
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
    val x73 = x44.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x72(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x77(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    val x78 = x73.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x77(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x83(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    val x84 = x78.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x83(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x89(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    val x90 = x84.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x89(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x95(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    val x96 = x90.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x95(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x105(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x104 = CPair.of(x103.asInstanceOf[java.lang.Integer], x102);
      x104: CPair[java.lang.Integer, LineItem]
    }
    val x106 = x96.parallelDo(new DoFn[LineItem, CPair[java.lang.Integer, LineItem]] {
      def process(input: LineItem, emitter: Emitter[CPair[java.lang.Integer, LineItem]]): Unit = {
        emitter.emit(x105(input))
      }
    }, Writables.tableOf(Writables.ints(), Writables.records(classOf[LineItem])));
    val x107 = joinNotNull(x106, x101);
    @inline
    def x122(x108: (CPair[java.lang.Integer, CPair[LineItem, Order]])) = {
      val x110 = x108.second();
      val x112 = x110.second();
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
      val x119 = CPair.of(x117.asInstanceOf[java.lang.Integer], x118.asInstanceOf[java.lang.Integer]);
      val x111 = x110.first();
      val x120 = x111.l_shipmode;
      val x121 = CPair.of(x120, x119);
      x121: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]
    }
    val x123 = x107.parallelDo(new DoFn[CPair[java.lang.Integer, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Integer, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {
        emitter.emit(x122(input))
      }
    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())));
    val x124 = x123.groupByKey;
    @inline
    def x134(x125: CPair[java.lang.Integer, java.lang.Integer], x126: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x127 = x125.first();
      val x129 = x126.first();
      val x131 = x127 + x129;
      val x128 = x125.second();
      val x130 = x126.second();
      val x132 = x128 + x130;
      val x133 = CPair.of(x131.asInstanceOf[java.lang.Integer], x132.asInstanceOf[java.lang.Integer]);
      x133: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x135 = x124.combineValues(new CombineWrapper(x134));
    @inline
    def x146(x136: (CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]])) = {
      val x138 = x136.second();
      val x142 = x138.second();
      val x137 = x136.first();
      val x139 = """shipmode """ + x137;
      val x140 = x139 + """: high """;
      val x141 = x138.first();
      val x143 = x140 + x141;
      val x144 = x143 + """, low """;
      val x145 = x144 + x142;
      x145: java.lang.String
    }
    val x147 = x135.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x146(input))
      }
    }, Writables.strings());
    val x148 = pipeline.writeTextFile(x147, x3);

    pipeline.done();
    return 0;
  }
}
// Types that are used in this program

case class LineItem(var l_orderkey: Int = 0, var l_partkey: Int = 0, var l_suppkey: Int = 0, var l_linenumber: Int = 0, var l_quantity: Double = 0, var l_extendedprice: Double = 0, var l_discount: Double = 0, var l_tax: Double = 0, var l_returnflag: Char = ' ', var l_linestatus: Char = ' ', var l_shipdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_commitdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_receiptdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_shipinstruct: java.lang.String = " ", var l_shipmode: java.lang.String = " ", var l_comment: java.lang.String = " ") extends Writable {
  def this() = this(l_orderkey = 0)

  var __bitset: Long = 65535;
  override def readFields(in: DataInput) {
    __bitset = WritableUtils.readVLong(in)
    __bitset match {
      case 65535 => readFields_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 65535 => write_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(in: DataInput) {
    l_orderkey = WritableUtils.readVInt(in)
    l_partkey = WritableUtils.readVInt(in)
    l_suppkey = WritableUtils.readVInt(in)
    l_linenumber = WritableUtils.readVInt(in)
    l_quantity = in.readDouble
    l_extendedprice = in.readDouble
    l_discount = in.readDouble
    l_tax = in.readDouble
    l_returnflag = in.readChar
    l_linestatus = in.readChar
    l_shipdate.readFields(in)
    l_commitdate.readFields(in)
    l_receiptdate.readFields(in)
    l_shipinstruct = in.readUTF
    l_shipmode = in.readUTF
    l_comment = in.readUTF
  }
  def write_0_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15(out: DataOutput) {
    WritableUtils.writeVInt(out, l_orderkey)
    WritableUtils.writeVInt(out, l_partkey)
    WritableUtils.writeVInt(out, l_suppkey)
    WritableUtils.writeVInt(out, l_linenumber)
    out.writeDouble(l_quantity)
    out.writeDouble(l_extendedprice)
    out.writeDouble(l_discount)
    out.writeDouble(l_tax)
    out.writeChar(l_returnflag)
    out.writeChar(l_linestatus)
    l_shipdate.write(out)
    l_commitdate.write(out)
    l_receiptdate.write(out)
    out.writeUTF(l_shipinstruct)
    out.writeUTF(l_shipmode)
    out.writeUTF(l_comment)
  }

}

case class Order(var o_orderkey: Int = 0, var o_custkey: Int = 0, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0)

  var __bitset: Long = 511;
  override def readFields(in: DataInput) {
    __bitset = WritableUtils.readVLong(in)
    __bitset match {
      case 511 => readFields_0_1_2_3_4_5_6_7_8(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 511 => write_0_1_2_3_4_5_6_7_8(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_0_1_2_3_4_5_6_7_8(in: DataInput) {
    o_orderkey = WritableUtils.readVInt(in)
    o_custkey = WritableUtils.readVInt(in)
    o_orderstatus = in.readChar
    o_totalprice = in.readDouble
    o_orderdate.readFields(in)
    o_orderpriority = in.readUTF
    o_clerk = in.readUTF
    o_shippriority = WritableUtils.readVInt(in)
    o_comment = in.readUTF
  }
  def write_0_1_2_3_4_5_6_7_8(out: DataOutput) {
    WritableUtils.writeVInt(out, o_orderkey)
    WritableUtils.writeVInt(out, o_custkey)
    out.writeChar(o_orderstatus)
    out.writeDouble(o_totalprice)
    o_orderdate.write(out)
    out.writeUTF(o_orderpriority)
    out.writeUTF(o_clerk)
    WritableUtils.writeVInt(out, o_shippriority)
    out.writeUTF(o_comment)
  }

}

class TaggedValue_LineItem_Order(left: Boolean, v1: LineItem, v2: Order) extends TaggedValue[LineItem, Order](left, v1, v2) {
  def this() = this(false, new LineItem(), new Order())
}
/**
 * ***************************************
 * End of Crunch Code
 * *****************************************
 */
