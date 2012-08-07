/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v2;

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
  // field reduction: true
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[TpchQueries], getConf());

    val x1 = args.drop(1);
    val x3 = x1(1);
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", false, false);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", false, false);
    @inline
    def x846(x108: (CPair[java.lang.Long, CPair[LineItem, Order]])) = {
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
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x2 = x1(0);
    val x44 = x2 + """/orders/""";
    val x45 = pipeline.readTextFile(x44);
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    @inline
    def x847(x46: (java.lang.String)) = {
      val x47 = x10.split(x46, 9);
      val x48 = x47(0);
      val x49 = x48.toLong;
      val x58 = x47(5);
      val x210 = new Order(o_orderkey = x49, o_orderpriority = x58);
      x210.__bitset = 33
      x210: Order
    }
    val x848 = x45.parallelDo(new DoFn[java.lang.String, Order] {
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {
        emitter.emit(x847(input))
      }
    }, Writables.records(classOf[Order]));
    @inline
    def x849(x95: (Order)) = {
      val x96 = x95.o_orderkey;
      val x181 = x95.o_orderpriority;
      val x182 = new Order(o_orderpriority = x181);
      x182.__bitset = 32
      val x183 = CPair.of(x96.asInstanceOf[java.lang.Long], x182);
      x183: CPair[java.lang.Long, Order]
    }
    val x850 = x848.parallelDo(new DoFn[Order, CPair[java.lang.Long, Order]] {
      def process(input: Order, emitter: Emitter[CPair[java.lang.Long, Order]]): Unit = {
        emitter.emit(x849(input))
      }
    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[Order])));
    @inline
    def x851(x100: (LineItem)) = {
      val x101 = x100.l_orderkey;
      val x255 = x100.l_shipmode;
      val x256 = new LineItem(l_shipmode = x255);
      x256.__bitset = 16384
      val x257 = CPair.of(x101.asInstanceOf[java.lang.Long], x256);
      x257: CPair[java.lang.Long, LineItem]
    }
    val x7 = x2 + """/lineitem/""";
    val x8 = pipeline.readTextFile(x7);
    @inline
    def x772(x9: (java.lang.String)) = {
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
      val x274 = new LineItem(l_orderkey = x13, l_shipdate = x33, l_commitdate = x35, l_receiptdate = x37, l_shipmode = x39);
      x274.__bitset = 23553
      x274: LineItem
    }
    val x773 = x8.parallelDo(new DoFn[java.lang.String, LineItem] {
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {
        emitter.emit(x772(input))
      }
    }, Writables.records(classOf[LineItem]));
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, false, false);
    @inline
    def x852(x66: (LineItem)) = {
      val x67 = x66.l_shipmode;
      val x69 = x68.matches(x67);
      x69: Boolean
    }
    val x853 = x773.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x852(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x854(x72: (LineItem)) = {
      val x73 = x72.l_receiptdate;
      val x74 = x5 <= x73;
      x74: Boolean
    }
    val x855 = x853.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x854(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x856(x77: (LineItem)) = {
      val x78 = x77.l_shipdate;
      val x79 = x77.l_commitdate;
      val x80 = x78 < x79;
      x80: Boolean
    }
    val x857 = x855.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x856(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x858(x83: (LineItem)) = {
      val x84 = x83.l_commitdate;
      val x85 = x83.l_receiptdate;
      val x86 = x84 < x85;
      x86: Boolean
    }
    val x859 = x857.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x858(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x860(x89: (LineItem)) = {
      val x90 = x89.l_receiptdate;
      val x92 = x90 < x91;
      x92: Boolean
    }
    val x861 = x859.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x860(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x862 = x861.parallelDo(new DoFn[LineItem, CPair[java.lang.Long, LineItem]] {
      def process(input: LineItem, emitter: Emitter[CPair[java.lang.Long, LineItem]]): Unit = {
        emitter.emit(x851(input))
      }
    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[LineItem])));
    val x863 = joinWritables(classOf[TaggedValue_LineItem_Order], x862, x850, x106);
    val x864 = x863.parallelDo(new DoFn[CPair[java.lang.Long, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Long, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {
        emitter.emit(x846(input))
      }
    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())));
    val x865 = x864.groupByKey(1);
    @inline
    def x892(x127: CPair[java.lang.Integer, java.lang.Integer], x128: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x129 = x127.first();
      val x131 = x128.first();
      val x133 = x129 + x131;
      val x130 = x127.second();
      val x132 = x128.second();
      val x134 = x130 + x132;
      val x135 = CPair.of(x133.asInstanceOf[java.lang.Integer], x134.asInstanceOf[java.lang.Integer]);
      x135: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x893 = x865.combineValues(new CombineWrapper(x892));
    @inline
    def x894(x138: (CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]])) = {
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
    val x895 = x893.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x894(input))
      }
    }, Writables.strings());
    val x896 = pipeline.writeTextFile(x895, x3);

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
      case 16384 => readFields_14(in)
      case 23553 => readFields_0_10_11_12_14(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 16384 => write_14(out)
      case 23553 => write_0_10_11_12_14(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_14(in: DataInput) {
    l_shipmode = in.readUTF
  }
  def write_14(out: DataOutput) {
    out.writeUTF(l_shipmode)
  }
  def readFields_0_10_11_12_14(in: DataInput) {
    l_orderkey = WritableUtils.readVLong(in)
    l_shipdate.readFields(in)
    l_commitdate.readFields(in)
    l_receiptdate.readFields(in)
    l_shipmode = in.readUTF
  }
  def write_0_10_11_12_14(out: DataOutput) {
    WritableUtils.writeVLong(out, l_orderkey)
    l_shipdate.write(out)
    l_commitdate.write(out)
    l_receiptdate.write(out)
    out.writeUTF(l_shipmode)
  }

}

case class Order(var o_orderkey: Long = 0L, var o_custkey: Long = 0L, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0L)

  var __bitset: Long = 511;
  override def readFields(in: DataInput) {
    __bitset = WritableUtils.readVLong(in)
    __bitset match {
      case 33 => readFields_0_5(in)
      case 32 => readFields_5(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 33 => write_0_5(out)
      case 32 => write_5(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_0_5(in: DataInput) {
    o_orderkey = WritableUtils.readVLong(in)
    o_orderpriority = in.readUTF
  }
  def write_0_5(out: DataOutput) {
    WritableUtils.writeVLong(out, o_orderkey)
    out.writeUTF(o_orderpriority)
  }
  def readFields_5(in: DataInput) {
    o_orderpriority = in.readUTF
  }
  def write_5(out: DataOutput) {
    out.writeUTF(o_orderpriority)
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
