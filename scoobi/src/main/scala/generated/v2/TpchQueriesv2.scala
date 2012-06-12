/**
 * ***************************************
 * Emitting Scoobi Code
 * *****************************************
 */

package dcdsl.generated.v2;
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
  // loop fusion: false
  // inline in loop fusion: false
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
    @inline
    def x1010(x136: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
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
    @inline
    def x896(x108: (scala.Tuple2[Int, scala.Tuple2[LineItem, Order]])) = {
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
    @inline
    def x906(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x179 = x97.o_orderpriority;
      val x180 = new Order(o_orderpriority = x179);
      x180.__bitset = 32
      val x181 = (x98, x180);
      x181: scala.Tuple2[Int, Order]
    }

    @inline
    def x897(x47: (java.lang.String), x11: RegexFrontend) = {
      val x48 = x11.split(x47, 9);
      val x49 = x48(0);
      val x50 = x49.toInt;
      val x59 = x48(5);
      val x208 = new Order(o_orderkey = x50, o_orderpriority = x59);
      x208.__bitset = 33
      x208: Order
    }
    val x2 = x1(0);
    val x45 = x2 + """/orders.tbl""";
    val x46 = TextInput.fromTextFile(x45);
    // x46
    val x905 = x46.parallelDo(new DoFn[java.lang.String, Order, Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {

        val x900 = input // loop var x898;
        val x901 = x897(x900, x11);
        emitter.emit(x901) // yield
        val x902 = ()
      }

      def cleanup(emitter: Emitter[Order]): Unit = {}
    })

    // x905
    val x914 = x905.parallelDo(new DoFn[Order, scala.Tuple2[Int, Order], Unit] {
      def setup(): Unit = {}
      def process(input: Order, emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {

        val x909 = input // loop var x907;
        val x910 = x906(x909);
        emitter.emit(x910) // yield
        val x911 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {}
    })

    @inline
    def x915(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x253 = x102.l_shipmode;
      val x254 = new LineItem(l_shipmode = x253);
      x254.__bitset = 16384
      val x255 = (x103, x254);
      x255: scala.Tuple2[Int, LineItem]
    }
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x977(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    @inline
    def x964(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    @inline
    def x951(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    @inline
    def x938(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x925(x67: (LineItem)) = {
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
    @inline
    def x916(x10: (java.lang.String), x11: RegexFrontend) = {
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
    val x8 = x2 + """/lineitem*""";
    val x9 = TextInput.fromTextFile(x8);
    // x9
    val x924 = x9.parallelDo(new DoFn[java.lang.String, LineItem, Unit] {
      val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {

        val x919 = input // loop var x917;
        val x920 = x916(x919, x11);
        emitter.emit(x920) // yield
        val x921 = ()
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x924
    val x937 = x924.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x928 = input // loop var x926;
        val x929 = x925(x928);
        val x934 = if (x929) {
          emitter.emit(x928) // yield
          val x930 = ()
          x930
        } else {
          val x932 = () // skip;
          x932
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x937
    val x950 = x937.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x941 = input // loop var x939;
        val x942 = x938(x941);
        val x947 = if (x942) {
          emitter.emit(x941) // yield
          val x943 = ()
          x943
        } else {
          val x945 = () // skip;
          x945
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x950
    val x963 = x950.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x954 = input // loop var x952;
        val x955 = x951(x954);
        val x960 = if (x955) {
          emitter.emit(x954) // yield
          val x956 = ()
          x956
        } else {
          val x958 = () // skip;
          x958
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x963
    val x976 = x963.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x967 = input // loop var x965;
        val x968 = x964(x967);
        val x973 = if (x968) {
          emitter.emit(x967) // yield
          val x969 = ()
          x969
        } else {
          val x971 = () // skip;
          x971
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x976
    val x989 = x976.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x980 = input // loop var x978;
        val x981 = x977(x980);
        val x986 = if (x981) {
          emitter.emit(x980) // yield
          val x982 = ()
          x982
        } else {
          val x984 = () // skip;
          x984
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x989
    val x997 = x989.parallelDo(new DoFn[LineItem, scala.Tuple2[Int, LineItem], Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {

        val x992 = input // loop var x990;
        val x993 = x915(x992);
        emitter.emit(x993) // yield
        val x994 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {}
    })

    val x998 = x997.join(x914);
    // x998
    val x1006 = x998.parallelDo(new DoFn[scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {

        val x1001 = input // loop var x999;
        val x1002 = x896(x1001);
        emitter.emit(x1002) // yield
        val x1003 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {}
    })

    val x1007 = x1006.groupByKey;
    @inline
    def x1008(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x1009 = x1007.combine(x1008);
    // x1009
    val x1018 = x1009.parallelDo(new DoFn[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], emitter: Emitter[java.lang.String]): Unit = {

        val x1013 = input // loop var x1011;
        val x1014 = x1010(x1013);
        emitter.emit(x1014) // yield
        val x1015 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    val x1019 = persist(TextOutput.toTextFile(x1018, x3));
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

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
