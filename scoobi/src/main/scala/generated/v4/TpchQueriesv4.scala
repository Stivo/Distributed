/**
 * ***************************************
 * Emitting Scoobi Code
 * *****************************************
 */

package dcdsl.generated.v4;
import ch.epfl.distributed.utils.WireFormatsGen
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat
import ch.epfl.distributed.datastruct._
import ch.epfl.distributed.utils._
import org.apache.hadoop.io.{ Writable, WritableUtils }
import java.io.{ DataInput, DataOutput }
object TpchQueries extends ScoobiApp {
  // field reduction: true
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
    val x3878 = x9.parallelDo(new DoFn[java.lang.String, scala.Tuple2[Int, LineItem], Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, true);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {

        val x919 = input // loop var x917;
        val x1432 = x11.split(x919, 16);
        val x1441 = x1432(14);
        val x3324 = x1441 == x6;
        val x3326 = if (x3324) {
          true
        } else {
          val x3325 = x1441 == x7;
          x3325
        }
        val x3876 = if (x3326) {
          val x1439 = x1432(12);
          val x1440 = ch.epfl.distributed.datastruct.Date(x1439);
          val x3633 = x5 <= x1440;
          val x3874 = if (x3633) {
            val x1435 = x1432(10);
            val x1436 = ch.epfl.distributed.datastruct.Date(x1435);
            val x1437 = x1432(11);
            val x1438 = ch.epfl.distributed.datastruct.Date(x1437);
            val x3634 = x1436 < x1438;
            val x3872 = if (x3634) {
              val x3822 = x1438 < x1440;
              val x3834 = if (x3822) {
                val x3823 = x1440 < x93;
                val x3830 = if (x3823) {
                  val x1433 = x1432(0);
                  val x1434 = x1433.toInt;
                  val x3824 = new LineItem(l_shipmode = x1441);
                  x3824.__bitset = 16384
                  val x3825 = (x1434, x3824);
                  emitter.emit(x3825) // yield
                  val x3826 = ()
                  x3826
                } else {
                  val x3828 = () // skip;
                  x3828
                }
                x3830
              } else {
                val x3832 = () // skip;
                x3832
              }
              x3834
            } else {
              val x3807 = () // skip;
              x3807
            }
            x3872
          } else {
            val x3812 = () // skip;
            x3812
          }
          x3874
        } else {
          val x3817 = () // skip;
          x3817
        }
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {}
    })

    val x45 = x2 + """/orders.tbl""";
    val x46 = TextInput.fromTextFile(x45);
    // x46
    val x3415 = x46.parallelDo(new DoFn[java.lang.String, scala.Tuple2[Int, Order], Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, true);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {

        val x900 = input // loop var x898;
        val x1024 = x11.split(x900, 9);
        val x1025 = x1024(0);
        val x1026 = x1025.toInt;
        val x1027 = x1024(5);
        val x3320 = new Order(o_orderpriority = x1027);
        x3320.__bitset = 32
        val x3321 = (x1026, x3320);
        emitter.emit(x3321) // yield
        val x3322 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {}
    })

    val x3879 = x3878.join(x3415);
    // x3879
    val x3896 = x3879.parallelDo(new DoFn[scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {

        val x3881 = input // loop var x999;
        val x3882 = x3881._2;
        val x3883 = x3882._2;
        val x3884 = x3883.o_orderpriority;
        val x3885 = x3884.startsWith("""1""");
        val x3887 = if (x3885) {
          true
        } else {
          val x3886 = x3884.startsWith("""2""");
          x3886
        }
        val x3888 = if (x3887) {
          1
        } else {
          0
        }
        val x3889 = 1 - x3888;
        val x3890 = (x3888, x3889);
        val x3891 = x3882._1;
        val x3892 = x3891.l_shipmode;
        val x3893 = (x3892, x3890);
        emitter.emit(x3893) // yield
        val x3894 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {}
    })

    val x3897 = x3896.groupByKey;
    @inline
    def x3288(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x3898 = x3897.combine(x3288);
    // x3898
    val x3912 = x3898.parallelDo(new DoFn[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], emitter: Emitter[java.lang.String]): Unit = {

        val x3900 = input // loop var x1011;
        val x3901 = x3900._2;
        val x3902 = x3901._2;
        val x3903 = x3900._1;
        val x3904 = """shipmode """ + x3903;
        val x3905 = x3904 + """: high """;
        val x3906 = x3901._1;
        val x3907 = x3905 + x3906;
        val x3908 = x3907 + """, low """;
        val x3909 = x3908 + x3902;
        emitter.emit(x3909) // yield
        val x3910 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    val x3913 = persist(TextOutput.toTextFile(x3912, x3));
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

case class Order(var o_orderkey: Int = 0, var o_custkey: Int = 0, var o_orderstatus: Char = ' ', var o_totalprice: Double = 0, var o_orderdate: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var o_orderpriority: java.lang.String = " ", var o_clerk: java.lang.String = " ", var o_shippriority: Int = 0, var o_comment: java.lang.String = " ") extends Writable {
  def this() = this(o_orderkey = 0)

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

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
