/**
 * ***************************************
 * Emitting Scoobi Code
 * *****************************************
 */

package dcdsl.generated.v0;
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
    def x411(x136: (scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]])) = {
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
    def x399(x108: (scala.Tuple2[Int, scala.Tuple2[LineItem, Order]])) = {
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
    def x306(x97: (Order)) = {
      val x98 = x97.o_orderkey;
      val x99 = (x98, x97);
      x99: scala.Tuple2[Int, Order]
    }
    @inline
    def x297(x47: (java.lang.String), x11: RegexFrontend) = {
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
    val x2 = x1(0);
    val x45 = x2 + """/orders.tbl""";
    val x46 = TextInput.fromTextFile(x45);
    // x46
    val x305 = x46.parallelDo(new DoFn[java.lang.String, Order, Unit] {
      lazy val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[Order]): Unit = {

        val x300 = input // loop var x298;
        val x301 = x297(x300, x11);
        emitter.emit(x301) // yield
        val x302 = ()
      }

      def cleanup(emitter: Emitter[Order]): Unit = {}
    })

    // x305
    val x314 = x305.parallelDo(new DoFn[Order, scala.Tuple2[Int, Order], Unit] {
      def setup(): Unit = {}
      def process(input: Order, emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {

        val x309 = input // loop var x307;
        val x310 = x306(x309);
        emitter.emit(x310) // yield
        val x311 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, Order]]): Unit = {}
    })

    @inline
    def x389(x102: (LineItem)) = {
      val x103 = x102.l_orderkey;
      val x104 = (x103, x102);
      x104: scala.Tuple2[Int, LineItem]
    }
    val x4 = x1(2);
    val x5 = ch.epfl.distributed.datastruct.Date(x4);
    val x93 = x5 + new ch.epfl.distributed.datastruct.Interval(1, 0, 0);
    @inline
    def x376(x91: (LineItem)) = {
      val x92 = x91.l_receiptdate;
      val x94 = x92 < x93;
      x94: Boolean
    }
    @inline
    def x363(x85: (LineItem)) = {
      val x86 = x85.l_commitdate;
      val x87 = x85.l_receiptdate;
      val x88 = x86 < x87;
      x88: Boolean
    }
    @inline
    def x350(x79: (LineItem)) = {
      val x80 = x79.l_shipdate;
      val x81 = x79.l_commitdate;
      val x82 = x80 < x81;
      x82: Boolean
    }
    @inline
    def x337(x74: (LineItem)) = {
      val x75 = x74.l_receiptdate;
      val x76 = x5 <= x75;
      x76: Boolean
    }
    val x6 = x1(3);
    val x7 = x1(4);
    @inline
    def x324(x67: (LineItem)) = {
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
    def x315(x10: (java.lang.String), x11: RegexFrontend) = {
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
    val x8 = x2 + """/lineitem*""";
    val x9 = TextInput.fromTextFile(x8);
    // x9
    val x323 = x9.parallelDo(new DoFn[java.lang.String, LineItem, Unit] {
      lazy val x11 = new ch.epfl.distributed.datastruct.RegexFrontend("""\|""", false, false);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[LineItem]): Unit = {

        val x318 = input // loop var x316;
        val x319 = x315(x318, x11);
        emitter.emit(x319) // yield
        val x320 = ()
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x323
    val x336 = x323.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x327 = input // loop var x325;
        val x328 = x324(x327);
        val x333 = if (x328) {
          emitter.emit(x327) // yield
          val x329 = ()
          x329
        } else {
          val x331 = () // skip;
          x331
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x336
    val x349 = x336.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x340 = input // loop var x338;
        val x341 = x337(x340);
        val x346 = if (x341) {
          emitter.emit(x340) // yield
          val x342 = ()
          x342
        } else {
          val x344 = () // skip;
          x344
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x349
    val x362 = x349.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x353 = input // loop var x351;
        val x354 = x350(x353);
        val x359 = if (x354) {
          emitter.emit(x353) // yield
          val x355 = ()
          x355
        } else {
          val x357 = () // skip;
          x357
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x362
    val x375 = x362.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x366 = input // loop var x364;
        val x367 = x363(x366);
        val x372 = if (x367) {
          emitter.emit(x366) // yield
          val x368 = ()
          x368
        } else {
          val x370 = () // skip;
          x370
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x375
    val x388 = x375.parallelDo(new DoFn[LineItem, LineItem, Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[LineItem]): Unit = {

        val x379 = input // loop var x377;
        val x380 = x376(x379);
        val x385 = if (x380) {
          emitter.emit(x379) // yield
          val x381 = ()
          x381
        } else {
          val x383 = () // skip;
          x383
        }
      }

      def cleanup(emitter: Emitter[LineItem]): Unit = {}
    })

    // x388
    val x397 = x388.parallelDo(new DoFn[LineItem, scala.Tuple2[Int, LineItem], Unit] {
      def setup(): Unit = {}
      def process(input: LineItem, emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {

        val x392 = input // loop var x390;
        val x393 = x389(x392);
        emitter.emit(x393) // yield
        val x394 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[Int, LineItem]]): Unit = {}
    })

    val x398 = x397.join(x314);
    // x398
    val x407 = x398.parallelDo(new DoFn[scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[Int, scala.Tuple2[LineItem, Order]], emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {

        val x402 = input // loop var x400;
        val x403 = x399(x402);
        emitter.emit(x403) // yield
        val x404 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]]]): Unit = {}
    })

    val x408 = x407.groupByKey;
    @inline
    def x409(x125: scala.Tuple2[Int, Int], x126: scala.Tuple2[Int, Int]) = {
      val x127 = x125._1;
      val x129 = x126._1;
      val x131 = x127 + x129;
      val x128 = x125._2;
      val x130 = x126._2;
      val x132 = x128 + x130;
      val x133 = (x131, x132);
      x133: scala.Tuple2[Int, Int]
    }
    val x410 = x408.combine(x409);
    // x410
    val x419 = x410.parallelDo(new DoFn[scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: scala.Tuple2[java.lang.String, scala.Tuple2[Int, Int]], emitter: Emitter[java.lang.String]): Unit = {

        val x414 = input // loop var x412;
        val x415 = x411(x414);
        emitter.emit(x415) // yield
        val x416 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    val x420 = persist(TextOutput.toTextFile(x419, x3));
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
