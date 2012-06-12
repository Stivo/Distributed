/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v2;

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
  // field reduction: true
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[TpchQueries], getConf());

    val x1 = args.drop(1);
    val x3 = x1(1);
    @inline
    def x844(x108: (CPair[java.lang.Integer, CPair[LineItem, Order]])) = {
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
    val x2 = x1(0);
    val x45 = x2 + """/orders.tbl""";
    val x46 = pipeline.readTextFile(x45);
    val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
    @inline
    def x845(x47: (java.lang.String)) = {
      val x48 = x11.split(x47, 9);
      val x49 = x48(0);
      val x50 = x49.toInt;
      val x59 = x48(5);
      val x208 = new Order(o_orderkey = x50, o_orderpriority = x59);
      x208.__bitset = 33
      x208: Order
    }
    val x846 = x46.parallelDo(new DoFn[java.lang.String, Order] {
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {
        emitter.emit(x845(input))
      }
    }, Writables.records(classOf[Order]));
    @inline
    def x847(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x179 = x97.o_orderpriority;
      val x180 = new Order(o_orderpriority = x179);
      x180.__bitset = 32
      val x181 = CPair.of(x98.asInstanceOf[java.lang.Integer], x180);
      x181: CPair[java.lang.Integer, Order]
    }
    val x848 = x846.parallelDo(new DoFn[Order, CPair[java.lang.Integer, Order]] {
      def process(input: Order, emitter: Emitter[CPair[java.lang.Integer, Order]]): Unit = {
        emitter.emit(x847(input))
      }
    }, Writables.tableOf(Writables.ints(), Writables.records(classOf[Order])));
    @inline
    def x849(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x253 = x102.l_shipmode;
      val x254 = new LineItem(l_shipmode = x253);
      x254.__bitset = 16384
      val x255 = CPair.of(x103.asInstanceOf[java.lang.Integer], x254);
      x255: CPair[java.lang.Integer, LineItem]
    }
    val x8 = x2 + """lineitem/*""";
    val x9 = pipeline.readTextFile(x8);
    @inline
    def x770(x10: (java.lang.String)) = {
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
      val x272 = new LineItem(l_orderkey = x14, l_shipdate = x34, l_commitdate = x36, l_receiptdate = x38, l_shipmode = x40);
      x272.__bitset = 23553
      x272: LineItem
    }
    val x771 = x9.parallelDo(new DoFn[java.lang.String, LineItem] {
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {
        emitter.emit(x770(input))
      }
    }, Writables.records(classOf[LineItem]));
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x850(x67: (LineItem)) = {
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
    val x851 = x771.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x850(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    @inline
    def x852(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    val x853 = x851.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x852(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x854(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    val x855 = x853.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x854(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    @inline
    def x856(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    val x857 = x855.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x856(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x858(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    val x859 = x857.parallelDo(new DoFn[LineItem, LineItem] {
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {
        if (x858(input))
          emitter.emit(input)
      }
    }, Writables.records(classOf[LineItem]));
    val x860 = x859.parallelDo(new DoFn[LineItem, CPair[java.lang.Integer, LineItem]] {
      def process(input: LineItem, emitter: Emitter[CPair[java.lang.Integer, LineItem]]): Unit = {
        emitter.emit(x849(input))
      }
    }, Writables.tableOf(Writables.ints(), Writables.records(classOf[LineItem])));
    val x861 = joinNotNull(x860, x848);
    val x862 = x861.parallelDo(new DoFn[CPair[java.lang.Integer, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Integer, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {
        emitter.emit(x844(input))
      }
    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())));
    val x863 = x862.groupByKey;
    @inline
    def x890(x125: CPair[java.lang.Integer, java.lang.Integer], x126: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x127 = x125.first();
      val x129 = x126.first();
      val x131 = x127 + x129;
      val x128 = x125.second();
      val x130 = x126.second();
      val x132 = x128 + x130;
      val x133 = CPair.of(x131.asInstanceOf[java.lang.Integer], x132.asInstanceOf[java.lang.Integer]);
      x133: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x891 = x863.combineValues(new CombineWrapper(x890));
    @inline
    def x892(x136: (CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]])) = {
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
    val x893 = x891.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x892(input))
      }
    }, Writables.strings());
    val x894 = pipeline.writeTextFile(x893, x3);

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
    l_orderkey = WritableUtils.readVInt(in)
    l_shipdate.readFields(in)
    l_commitdate.readFields(in)
    l_receiptdate.readFields(in)
    l_shipmode = in.readUTF
  }
  def write_0_10_11_12_14(out: DataOutput) {
    WritableUtils.writeVInt(out, l_orderkey)
    l_shipdate.write(out)
    l_commitdate.write(out)
    l_receiptdate.write(out)
    out.writeUTF(l_shipmode)
  }

}

case class Order(var o_orderkey: Int = 0, var o_custkey: Int = 0, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0)

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
    o_orderkey = WritableUtils.readVInt(in)
    o_orderpriority = in.readUTF
  }
  def write_0_5(out: DataOutput) {
    WritableUtils.writeVInt(out, o_orderkey)
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
