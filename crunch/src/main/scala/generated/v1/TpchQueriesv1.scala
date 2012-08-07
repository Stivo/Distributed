/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v1;

import java.io.DataInput
import java.io.DataOutput
import java.io.Serializable

import scala.collection.JavaConversions._

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
import ch.epfl.distributed.utils.PartitionerUtil._

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
    val x44 = x2 + """/orders/""";
    val x45 = pipeline.readTextFile(x44);
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", true, true);
    @inline
    def x64(x46: (java.lang.String)) = {
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
      val x63 = new Order(o_orderkey = x49, o_custkey = x51, o_orderstatus = x53, o_totalprice = x55, o_orderdate = x57, o_orderpriority = x58, o_clerk = x59, o_shippriority = x61, o_comment = x62);
      x63.__bitset = 511
      x63: Order
    }
    val x65 = x45.parallelDo(new DoFn[java.lang.String, Order] {
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {
        emitter.emit(x64(input))
      }
    }, Writables.records(classOf[Order]));
    @inline
    def x98(x95: (Order)) = {
      val x96 = x95.o_orderkey;
      val x97 = CPair.of(x96.asInstanceOf[java.lang.Long], x95);
      x97: CPair[java.lang.Long, Order]
    }
    val x99 = x65.parallelDo(new DoFn[Order, CPair[java.lang.Long, Order]] {
      def process(input: Order, emitter: Emitter[CPair[java.lang.Long, Order]]): Unit = {
        emitter.emit(x98(input))
      }
    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[Order])));
    val x7 = x2 + """/lineitem/""";
    val x8 = pipeline.readTextFile(x7);
    @inline
    def x42(x9: (java.lang.String)) = {
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
      val x41 = new LineItem(l_orderkey = x13, l_partkey = x15, l_suppkey = x17, l_linenumber = x19, l_quantity = x21, l_extendedprice = x23, l_discount = x25, l_tax = x27, l_returnflag = x29, l_linestatus = x31, l_shipdate = x33, l_commitdate = x35, l_receiptdate = x37, l_shipinstruct = x38, l_shipmode = x39, l_comment = x40);
      x41.__bitset = 65535
      x41: LineItem
    }
    val x43 = x8.parallelDo(new DoFn[java.lang.String, LineItem] {
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {
        emitter.emit(x42(input))
      }
    }, Writables.records(classOf[LineItem]));
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, true, true);
    @inline
    def x70(x66: (LineItem)) = {
      val x67 = x66.l_shipmode;
      val x69 = x68.matches(x67);
      x69: Boolean
    }
    val x71 = x43.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x70(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x75(x72: (LineItem)) = {
      val x73 = x72.l_receiptdate;
      val x74 = x5 <= x73;
      x74: Boolean
    }
    val x76 = x71.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x75(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x81(x77: (LineItem)) = {
      val x78 = x77.l_shipdate;
      val x79 = x77.l_commitdate;
      val x80 = x78 < x79;
      x80: Boolean
    }
    val x82 = x76.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x81(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x87(x83: (LineItem)) = {
      val x84 = x83.l_commitdate;
      val x85 = x83.l_receiptdate;
      val x86 = x84 < x85;
      x86: Boolean
    }
    val x88 = x82.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x87(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x93(x89: (LineItem)) = {
      val x90 = x89.l_receiptdate;
      val x92 = x90 < x91;
      x92: Boolean
    }
    val x94 = x88.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x93(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x103(x100: (LineItem)) = {
      val x101 = x100.l_orderkey;
      val x102 = CPair.of(x101.asInstanceOf[java.lang.Long], x100);
      x102: CPair[java.lang.Long, LineItem]
    }
    val x104 = x94.parallelDo(new DoFn[LineItem, CPair[java.lang.Long, LineItem]] {
      def process(input: LineItem, emitter: Emitter[CPair[java.lang.Long, LineItem]]): Unit = {
        emitter.emit(x103(input))
      }
    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[LineItem])));
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x107 = joinWritables(classOf[TaggedValue_LineItem_Order], x104, x99, x106);
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", true, true);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", true, true);
    @inline
    def x124(x108: (CPair[java.lang.Long, CPair[LineItem, Order]])) = {
      val x110 = x108.second();
      val x112 = x110.second();
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
      val x121 = CPair.of(x119.asInstanceOf[java.lang.Integer], x120.asInstanceOf[java.lang.Integer]);
      val x111 = x110.first();
      val x122 = x111.l_shipmode;
      val x123 = CPair.of(x122, x121);
      x123: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]
    }
    val x125 = x107.parallelDo(new DoFn[CPair[java.lang.Long, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Long, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {
        emitter.emit(x124(input))
      }
    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())));
    val x126 = x125.groupByKey(1);
    @inline
    def x136(x127: CPair[java.lang.Integer, java.lang.Integer], x128: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x129 = x127.first();
      val x131 = x128.first();
      val x133 = x129 + x131;
      val x130 = x127.second();
      val x132 = x128.second();
      val x134 = x130 + x132;
      val x135 = CPair.of(x133.asInstanceOf[java.lang.Integer], x134.asInstanceOf[java.lang.Integer]);
      x135: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x137 = x126.combineValues(new CombineWrapper(x136));
    @inline
    def x148(x138: (CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]])) = {
      val x140 = x138.second();
      val x144 = x140.second();
      val x139 = x138.first();
      val x141 = """shipmode """ + x139;
      val x142 = x141 + """: high """;
      val x143 = x140.first();
      val x145 = x142 + x143;
      val x146 = x145 + """, low """;
      val x147 = x146 + x144;
      x147: java.lang.String
    }
    val x149 = x137.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x148(input))
      }
    }, Writables.strings());
    val x150 = pipeline.writeTextFile(x149, x3);

    pipeline.done();
    return 0;
  }
}
// Types that are used in this program

case class LineItem(var l_orderkey: Long = 0L, var l_partkey: Long = 0L, var l_suppkey: Long = 0L, var l_linenumber: Long = 0L, var l_quantity: Double = 0, var l_extendedprice: Double = 0, var l_discount: Double = 0, var l_tax: Double = 0, var l_returnflag: Char = ' ', var l_linestatus: Char = ' ', var l_shipdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_commitdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_receiptdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var l_shipinstruct: java.lang.String = " ", var l_shipmode: java.lang.String = " ", var l_comment: java.lang.String = " ") extends Writable {
  def this() = this(l_orderkey = 0L)

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
    l_orderkey = WritableUtils.readVLong(in)
    l_partkey = WritableUtils.readVLong(in)
    l_suppkey = WritableUtils.readVLong(in)
    l_linenumber = WritableUtils.readVLong(in)
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
    WritableUtils.writeVLong(out, l_orderkey)
    WritableUtils.writeVLong(out, l_partkey)
    WritableUtils.writeVLong(out, l_suppkey)
    WritableUtils.writeVLong(out, l_linenumber)
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

case class Order(var o_orderkey: Long = 0L, var o_custkey: Long = 0L, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0L)

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
    o_orderkey = WritableUtils.readVLong(in)
    o_custkey = WritableUtils.readVLong(in)
    o_orderstatus = in.readChar
    o_totalprice = in.readDouble
    o_orderdate.readFields(in)
    o_orderpriority = in.readUTF
    o_clerk = in.readUTF
    o_shippriority = WritableUtils.readVInt(in)
    o_comment = in.readUTF
  }
  def write_0_1_2_3_4_5_6_7_8(out: DataOutput) {
    WritableUtils.writeVLong(out, o_orderkey)
    WritableUtils.writeVLong(out, o_custkey)
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
