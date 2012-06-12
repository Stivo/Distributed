/**
 * ***************************************
 * Emitting Scoobi Code
 * *****************************************
 */

package dcdsl.generated.v3;
import ch.epfl.distributed.utils.WireFormatsGen
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat
import ch.epfl.distributed.datastruct._
import ch.epfl.distributed.utils._
import org.apache.hadoop.io.{ Writable, WritableUtils }
import java.io.{ DataInput, DataOutput }
object TpchQueries extends ScoobiApp {
  // field reduction: false
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  def mkAbstractWireFormat1[T, A <: T: Manifest: WireFormat](): WireFormat[T] = new WireFormat[T] {
    import java.io.{ DataOutput, DataInput }

    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
    }

    override def fromWire(in: DataInput): T =
      implicitly[WireFormat[A]].fromWire(in)
  }

  def makeGrouping[A] = new Grouping[A] {
    def groupCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
  }

  implicit def WritableFmt2[T <: Serializable with Writable: Manifest] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) { x.write(out) }
    def fromWire(in: DataInput): T = {
      val x: T = implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T]
      x.readFields(in)
      x
    }
  }

  def run() {
    val scoobiInputArgs = args
    import WireFormat.{ mkAbstractWireFormat }
    import WireFormatsGen.{ mkCaseWireFormatGen }
    implicit val grouping_date = makeGrouping[Date]
    implicit val grouping_simpledate = makeGrouping[SimpleDate]
    implicit val grouping_datetime = makeGrouping[DateTime]

    val x1 = scoobiInputArgs;
    val x3 = x1(1);
    val x2 = x1(0);
    val x8 = x2 + """/lineitem*""";
    val x9 = TextInput.fromTextFile(x8);
    // x9
    val x6 = x1(3);
    val x7 = x1(4);
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    val x3245 = x9.parallelDo(new DoFn[java.lang.String, scala.Tuple2[Int, LineItem], Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);

      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {

        val x318 = input // loop var x316;
        val x839 = x11.split(x318, 16);
        val x867 = x839(14);
        val x2704 = x867 == x6;
        val x2706 = if (x2704) {
          true
        } else {
          val x2705 = x867 == x7;
          x2705
        }
        val x3243 = if (x2706) {
          val x864 = x839(12);
          val x865 = ch.epfl.distributed.datastruct.Date(x864);
          val x3005 = x5 <= x865;
          val x3241 = if (x3005) {
            val x860 = x839(10);
            val x861 = ch.epfl.distributed.datastruct.Date(x860);
            val x862 = x839(11);
            val x863 = ch.epfl.distributed.datastruct.Date(x862);
            val x3006 = x861 < x863;
            val x3239 = if (x3006) {
              val x3190 = x863 < x865;
              val x3201 = if (x3190) {
                val x3191 = x865 < x93;
                val x3197 = if (x3191) {
                  val x840 = x839(0);
                  val x841 = x840.toInt;
                  val x842 = x839(1);
                  val x843 = x842.toInt;
                  val x844 = x839(2);
                  val x845 = x844.toInt;
                  val x846 = x839(3);
                  val x847 = x846.toInt;
                  val x848 = x839(4);
                  val x849 = x848.toDouble;
                  val x850 = x839(5);
                  val x851 = x850.toDouble;
                  val x852 = x839(6);
                  val x853 = x852.toDouble;
                  val x854 = x839(7);
                  val x855 = x854.toDouble;
                  val x856 = x839(8);
                  val x857 = x856.charAt(0);
                  val x858 = x839(9);
                  val x859 = x858.charAt(0);
                  val x866 = x839(13);
                  val x868 = x839(15);
                  val x869 = new LineItem(l_orderkey = x841, l_partkey = x843, l_suppkey = x845, l_linenumber = x847, l_quantity = x849, l_extendedprice = x851, l_discount = x853, l_tax = x855, l_returnflag = x857, l_linestatus = x859, l_shipdate = x861, l_commitdate = x863, l_receiptdate = x865, l_shipinstruct = x866, l_shipmode = x867, l_comment = x868);
                  x869.__bitset = 65535
                  val x3192 = (x841, x869);
                  emitter.emit(x3192) // yield
                  val x3193 = ()
                  x3193
                } else {
                  val x3195 = () // skip;
                  x3195
                }
                x3197
              } else {
                val x3199 = () // skip;
                x3199
              }
              x3201
            } else {
              val x3175 = () // skip;
              x3175
            }
            x3239
          } else {
            val x3180 = () // skip;
            x3180
          }
          x3241
        } else {
          val x3185 = () // skip;
          x3185
        }
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {}
    })

    val x45 = x2 + """/orders.tbl""";
    val x46 = TextInput.fromTextFile(x45);
    // x46
    val x2793 = x46.parallelDo(new DoFn[java.lang.String, scala.Tuple2[Int, Order], Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);

      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {

        val x300 = input // loop var x298;
        val x425 = x11.split(x300, 9);
        val x426 = x425(0);
        val x427 = x426.toInt;
        val x428 = x425(1);
        val x429 = x428.toInt;
        val x430 = x425(2);
        val x431 = x430.charAt(0);
        val x432 = x425(3);
        val x433 = x432.toDouble;
        val x434 = x425(4);
        val x435 = ch.epfl.distributed.datastruct.Date(x434);
        val x436 = x425(5);
        val x437 = x425(6);
        val x438 = x425(7);
        val x439 = x438.toInt;
        val x440 = x425(8);
        val x441 = new Order(o_orderkey = x427, o_custkey = x429, o_orderstatus = x431, o_totalprice = x433, o_orderdate = x435, o_orderpriority = x436, o_clerk = x437, o_shippriority = x439, o_comment = x440);
        x441.__bitset = 511
        val x2701 = (x427, x441);
        emitter.emit(x2701) // yield
        val x2702 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {}
    })

    val x3246 = x3245.join(x2793);
    // x3246
    val x3263 = x3246.parallelDo(new DoFn[scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {

        val x3248 = input // loop var x400;
        val x3249 = x3248._2;
        val x3250 = x3249._2;
        val x3251 = x3250.o_orderpriority;
        val x3252 = x3251.startsWith("""1""");
        val x3254 = if (x3252) {
          true
        } else {
          val x3253 = x3251.startsWith("""2""");
          x3253
        }
        val x3255 = if (x3254) {
          1
        } else {
          0
        }
        val x3256 = 1 - x3255;
        val x3257 = (x3255, x3256);
        val x3258 = x3249._1;
        val x3259 = x3258.l_shipmode;
        val x3260 = (x3259, x3257);
        emitter.emit(x3260) // yield
        val x3261 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {}
    })

    val x3264 = x3263.groupByKey;
    @inline
    def x2669(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x3265 = x3264.combine(x2669);
    // x3265
    val x3279 = x3265.parallelDo(new DoFn[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], emitter: Emitter[java.lang.String]): Unit = {

        val x3267 = input // loop var x412;
        val x3268 = x3267._2;
        val x3269 = x3268._2;
        val x3270 = x3267._1;
        val x3271 = """shipmode """ + x3270;
        val x3272 = x3271 + """: high """;
        val x3273 = x3268._1;
        val x3274 = x3272 + x3273;
        val x3275 = x3274 + """, low """;
        val x3276 = x3275 + x3269;
        emitter.emit(x3276) // yield
        val x3277 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    val x3280 = persist(TextOutput.toTextFile(x3279, x3));
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

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
