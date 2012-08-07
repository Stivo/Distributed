/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v4;

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
    val x10 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", true, true);
    val x6 = x1(3);
    val x68 = new ch.epfl.distributed.datastruct.RegexFrontend(x6, true, true);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x91 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3846 = x8.parallelDo(new DoFn[java.lang.String, CPair[java.lang.Long, LineItem]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.Long, LineItem]]): Unit = {

        val x921 = input // loop var x919;
        val x1434 = x10.split(x921, 16);
        val x1443 = x1434(14);
        val x3294 = x68.matches(x1443);
        val x3844 = if (x3294) {
          val x1441 = x1434(12);
          val x1442 = ch.epfl.distributed.datastruct.Date(x1441);
          val x3601 = x5 <= x1442;
          val x3842 = if (x3601) {
            val x1437 = x1434(10);
            val x1438 = ch.epfl.distributed.datastruct.Date(x1437);
            val x1439 = x1434(11);
            val x1440 = ch.epfl.distributed.datastruct.Date(x1439);
            val x3602 = x1438 < x1440;
            val x3840 = if (x3602) {
              val x3790 = x1440 < x1442;
              val x3802 = if (x3790) {
                val x3791 = x1442 < x91;
                val x3798 = if (x3791) {
                  val x1435 = x1434(0);
                  val x1436 = x1435.toLong;
                  val x3792 = new LineItem(l_shipmode = x1443);
                  x3792.__bitset = 16384
                  val x3793 = CPair.of(x1436.asInstanceOf[java.lang.Long], x3792);
                  emitter.emit(x3793) // yield
                  val x3794 = ()
                  x3794
                } else {
                  val x3796 = () // skip;
                  x3796
                }
                x3798
              } else {
                val x3800 = () // skip;
                x3800
              }
              x3802
            } else {
              val x3775 = () // skip;
              x3775
            }
            x3840
          } else {
            val x3780 = () // skip;
            x3780
          }
          x3842
        } else {
          val x3785 = () // skip;
          x3785
        }
      }

    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[LineItem])))
    val x44 = x2 + """/orders/""";
    val x45 = pipeline.readTextFile(x44);
    // x45
    val x3383 = x45.parallelDo(new DoFn[java.lang.String, CPair[java.lang.Long, Order]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.Long, Order]]): Unit = {

        val x902 = input // loop var x900;
        val x1026 = x10.split(x902, 9);
        val x1027 = x1026(0);
        val x1028 = x1027.toLong;
        val x1029 = x1026(5);
        val x3290 = new Order(o_orderpriority = x1029);
        x3290.__bitset = 32
        val x3291 = CPair.of(x1028.asInstanceOf[java.lang.Long], x3290);
        emitter.emit(x3291) // yield
        val x3292 = ()
      }

    }, Writables.tableOf(Writables.longs(), Writables.records(classOf[Order])))
    val x3847 = joinWritables(classOf[TaggedValue_LineItem_Order], x3846, x3383, x106);
    // x3847
    val x114 = new ch.epfl.distributed.datastruct.RegexFrontend("""1-URGENT""", true, true);
    val x116 = new ch.epfl.distributed.datastruct.RegexFrontend("""2-HIGH""", true, true);
    val x3864 = x3847.parallelDo(new DoFn[CPair[java.lang.Long, CPair[LineItem, Order]], CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]] {
      def process(input: CPair[java.lang.Long, CPair[LineItem, Order]], emitter: Emitter[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]]]): Unit = {

        val x3849 = input // loop var x1001;
        val x3850 = x3849.second();
        val x3851 = x3850.second();
        val x3852 = x3851.o_orderpriority;
        val x3853 = x114.matches(x3852);
        val x3855 = if (x3853) {
          true
        } else {
          val x3854 = x116.matches(x3852);
          x3854
        }
        val x3856 = if (x3855) {
          1
        } else {
          0
        }
        val x3857 = 1 - x3856;
        val x3858 = CPair.of(x3856.asInstanceOf[java.lang.Integer], x3857.asInstanceOf[java.lang.Integer]);
        val x3859 = x3850.first();
        val x3860 = x3859.l_shipmode;
        val x3861 = CPair.of(x3860, x3858);
        emitter.emit(x3861) // yield
        val x3862 = ()
      }

    }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.ints(), Writables.ints())))
    val x3865 = x3864.groupByKey(1);
    @inline
    def x3258(x127: CPair[java.lang.Integer, java.lang.Integer], x128: CPair[java.lang.Integer, java.lang.Integer]) = {
      val x129 = x127.first();
      val x131 = x128.first();
      val x133 = x129 + x131;
      val x130 = x127.second();
      val x132 = x128.second();
      val x134 = x130 + x132;
      val x135 = CPair.of(x133.asInstanceOf[java.lang.Integer], x134.asInstanceOf[java.lang.Integer]);
      x135: CPair[java.lang.Integer, java.lang.Integer]
    }
    val x3866 = x3865.combineValues(new CombineWrapper(x3258));
    // x3866
    val x3880 = x3866.parallelDo(new DoFn[CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], java.lang.String] {
      def process(input: CPair[java.lang.String, CPair[java.lang.Integer, java.lang.Integer]], emitter: Emitter[java.lang.String]): Unit = {

        val x3868 = input // loop var x1013;
        val x3869 = x3868.second();
        val x3870 = x3869.second();
        val x3871 = x3868.first();
        val x3872 = """shipmode """ + x3871;
        val x3873 = x3872 + """: high """;
        val x3874 = x3869.first();
        val x3875 = x3873 + x3874;
        val x3876 = x3875 + """, low """;
        val x3877 = x3876 + x3870;
        emitter.emit(x3877) // yield
        val x3878 = ()
      }

    }, Writables.strings())
    val x3881 = pipeline.writeTextFile(x3880, x3);

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
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 16384 => write_14(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_14(in: DataInput) {
    l_shipmode = in.readUTF
  }
  def write_14(out: DataOutput) {
    out.writeUTF(l_shipmode)
  }

}

case class Order(var o_orderkey: Long = 0L, var o_custkey: Long = 0L, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0L)

  var __bitset: Long = 511;
  override def readFields(in: DataInput) {
    __bitset = WritableUtils.readVLong(in)
    __bitset match {
      case 32 => readFields_5(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 32 => write_5(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
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
