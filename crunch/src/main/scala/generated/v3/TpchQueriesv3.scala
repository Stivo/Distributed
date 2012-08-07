/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v3;

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
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[TpchQueries], getConf());

    val x1 = args.drop(1);
    val x3 = x1(1);
    val x105 = x1(4);
    val x106 = x105.toInt;
    val x2 = x1(0);
    val x7 = x2 + """/lineitem/""";
    val x8 = pipeline.readTextFile(x7);
    // x8
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", true, false);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, true, false);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3213 = x8.parallelDo(new DoFn[java.lang.String, CPair[java.lang.Long, LineItem]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.Long, LineItem]]): Unit = {

        val x320 = input // loop var x318;
        val x841 = x10.split(x320, 16);
        val x869 = x841(14);
        val x2674 = x68.matches(x869);
        val x3211 = if (x2674) {
          val x866 = x841(12);
          val x867 = ch.epfl.distributed.datastruct.Date(x866);
          val x2973 = x5 <= x867;
          val x3209 = if (x2973) {
            val x862 = x841(10);
            val x863 = ch.epfl.distributed.datastruct.Date(x862);
            val x864 = x841(11);
            val x865 = ch.epfl.distributed.datastruct.Date(x864);
            val x2974 = x863 < x865;
            val x3207 = if (x2974) {
              val x3158 = x865 < x867;
              val x3169 = if (x3158) {
                val x3159 = x867 < x91;
                val x3165 = if (x3159) {
                  val x842 = x841(0);
                  val x843 = x842.toLong;
                  val x844 = x841(1);
                  val x845 = x844.toLong;
                  val x846 = x841(2);
                  val x847 = x846.toLong;
                  val x848 = x841(3);
                  val x849 = x848.toLong;
                  val x850 = x841(4);
                  val x851 = x850.toDouble;
                  val x852 = x841(5);
                  val x853 = x852.toDouble;
                  val x854 = x841(6);
                  val x855 = x854.toDouble;
                  val x856 = x841(7);
                  val x857 = x856.toDouble;
                  val x858 = x841(8);
                  val x859 = x858.charAt(0);
                  val x860 = x841(9);
                  val x861 = x860.charAt(0);
                  val x868 = x841(13);
                  val x870 = x841(15);
                  val x871 = new LineItem(l_orderkey = x843, l_partkey = x845, l_suppkey = x847, l_linenumber = x849, l_quantity = x851, l_extendedprice = x853, l_discount = x855, l_tax = x857, l_returnflag = x859, l_linestatus = x861, l_shipdate = x863, l_commitdate = x865, l_receiptdate = x867, l_shipinstruct = x868, l_shipmode = x869, l_comment = x870);
                  x871.__bitset = 65535
                  val x3160 = CPair.of(x843.asInstanceOf[java.lang.Long], x871);
                  emitter.emit(x3160) // yield
                  val x3161 = ()
                  x3161
                } else {
                  val x3163 = () // skip;
                  x3163
                }
                x3165
              } else {
                val x3167 = () // skip;
                x3167
              }
              x3169
            } else {
              val x3143 = () // skip;
              x3143
            }
            x3207
          } else {
            val x3148 = () // skip;
            x3148
          }
          x3209
        } else {
          val x3153 = () // skip;
          x3153
        }
      }

    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[LineItem])))
    val x44 = x2 + """/orders/""";
    val x45 = pipeline.readTextFile(x44);
    // x45
    val x2761 = x45.parallelDo(new DoFn[java.lang.String, CPair[java.lang.Long, Order]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.Long, Order]]): Unit = {

        val x302 = input // loop var x300;
        val x427 = x10.split(x302, 9);
        val x428 = x427(0);
        val x429 = x428.toLong;
        val x430 = x427(1);
        val x431 = x430.toLong;
        val x432 = x427(2);
        val x433 = x432.charAt(0);
        val x434 = x427(3);
        val x435 = x434.toDouble;
        val x436 = x427(4);
        val x437 = ch.epfl.distributed.datastruct.Date(x436);
        val x438 = x427(5);
        val x439 = x427(6);
        val x440 = x427(7);
        val x441 = x440.toInt;
        val x442 = x427(8);
        val x443 = new Order(o_orderkey = x429, o_custkey = x431, o_orderstatus = x433, o_totalprice = x435, o_orderdate = x437, o_orderpriority = x438, o_clerk = x439, o_shippriority = x441, o_comment = x442);
        x443.__bitset = 511
        val x2671 = CPair.of(x429.asInstanceOf[java.lang.Long], x443);
        emitter.emit(x2671) // yield
        val x2672 = ()
      }

    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[Order])))
    val x3214 = joinWritables(classOf[TaggedValue_LineItem_Order], x3213, x2761, x106);
    // x3214
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", true, false);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", true, false);
    val x3231 = x3214.parallelDo(new DoFn[CPair[java.lang.Long, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Long, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {

        val x3216 = input // loop var x402;
        val x3217 = x3216.second();
        val x3218 = x3217.second();
        val x3219 = x3218.o_orderpriority;
        val x3220 = x114.matches(x3219);
        val x3222 = if (x3220) {
          true
        } else {
          val x3221 = x116.matches(x3219);
          x3221
        }
        val x3223 = if (x3222) {
          1
        } else {
          0
        }
        val x3224 = 1 - x3223;
        val x3225 = CPair.of(x3223.asInstanceOf[java.lang.Integer], x3224.asInstanceOf[java.lang.Integer]);
        val x3226 = x3217.first();
        val x3227 = x3226.l_shipmode;
        val x3228 = CPair.of(x3227, x3225);
        emitter.emit(x3228) // yield
        val x3229 = ()
      }

    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())))
    val x3232 = x3231.groupByKey(1);
    @inline
    def x2639(x127: CPair[java.lang.Integer, java.lang.Integer], x128: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x129 = x127.first();
      val x131 = x128.first();
      val x133 = x129 + x131;
      val x130 = x127.second();
      val x132 = x128.second();
      val x134 = x130 + x132;
      val x135 = CPair.of(x133.asInstanceOf[java.lang.Integer], x134.asInstanceOf[java.lang.Integer]);
      x135: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x3233 = x3232.combineValues(new CombineWrapper(x2639));
    // x3233
    val x3247 = x3233.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {

        val x3235 = input // loop var x414;
        val x3236 = x3235.second();
        val x3237 = x3236.second();
        val x3238 = x3235.first();
        val x3239 = """shipmode """ + x3238;
        val x3240 = x3239 + """: high """;
        val x3241 = x3236.first();
        val x3242 = x3240 + x3241;
        val x3243 = x3242 + """, low """;
        val x3244 = x3243 + x3237;
        emitter.emit(x3244) // yield
        val x3245 = ()
      }

    }, Writables.strings())
    val x3248 = pipeline.writeTextFile(x3247, x3);

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
