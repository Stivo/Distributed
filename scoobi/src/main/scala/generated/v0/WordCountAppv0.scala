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
object WordCountApp extends ScoobiApp {
  // field reduction: false
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: false
  // using writables: true
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
    val x54 = x1(1);
    @inline
    def x175(x44: (java.lang.String)) = {
      val x45 = (x44, 1);
      x45: scala.Tuple2[java.lang.String, Int]
    }
    @inline
    def x162(x39: (java.lang.String)) = {
      val x40 = x39.matches("""(thumb|left|right|\d+px){2,}""");
      val x41 = !x40;
      x41: Boolean
    }
    @inline
    def x149(x34: (java.lang.String)) = {
      val x35 = x34.length;
      val x36 = x35 > 1;
      x36: Boolean
    }
    @inline
    def x135(x29: (java.lang.String)) = {
      val x30 = x29.split("""[^a-zA-Z0-9']+""", 0);
      val x31 = x30.toSeq;
      x31: scala.collection.Seq[java.lang.String]
    }
    @inline
    def x126(x25: (java.lang.String)) = {
      val x26 = x25.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
      x26: java.lang.String
    }
    @inline
    def x117(x21: (java.lang.String)) = {
      val x22 = x21.replaceAll("""\[\[.*?\]\]""", """ """);
      x22: java.lang.String
    }
    @inline
    def x108(x16: (WikiArticle)) = {
      val x17 = x16.plaintext;
      val x18 = """\n""" + x17;
      x18: java.lang.String
    }
    @inline
    def x99(x4: (java.lang.String)) = {
      val x5 = x4.split("""	""", 5);
      val x6 = x5(0);
      val x7 = x6.toLong;
      val x8 = x5(1);
      val x9 = x5(2);
      val x10 = ch.epfl.distributed.datastruct.Date(x9);
      val x11 = x5(3);
      val x12 = x5(4);
      val x13 = new WikiArticle(pageId = x7, name = x8, updated = x10, xml = x11, plaintext = x12);
      x13.__bitset = 31
      x13: WikiArticle
    }
    val x2 = x1(0);
    val x3 = TextInput.fromTextFile(x2);
    // x3
    val x107 = x3.parallelDo(new DoFn[java.lang.String, WikiArticle, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[WikiArticle]): Unit = {

        val x102 = input // loop var x100;
        val x103 = x99(x102);
        emitter.emit(x103) // yield
        val x104 = ()
      }

      def cleanup(emitter: Emitter[WikiArticle]): Unit = {}
    })

    // x107
    val x116 = x107.parallelDo(new DoFn[WikiArticle, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: WikiArticle, emitter: Emitter[java.lang.String]): Unit = {

        val x111 = input // loop var x109;
        val x112 = x108(x111);
        emitter.emit(x112) // yield
        val x113 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x116
    val x125 = x116.parallelDo(new DoFn[java.lang.String, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {

        val x120 = input // loop var x118;
        val x121 = x117(x120);
        emitter.emit(x121) // yield
        val x122 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x125
    val x134 = x125.parallelDo(new DoFn[java.lang.String, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {

        val x129 = input // loop var x127;
        val x130 = x126(x129);
        emitter.emit(x130) // yield
        val x131 = ()
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x134
    val x148 = x134.parallelDo(new DoFn[java.lang.String, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {

        val x138 = input // loop var x136;
        val x139 = x135(x138);
        // x139
        {
          val it = x139.iterator
          while (it.hasNext) { // flatMap
            val input = it.next()
            val x142 = input // loop var x141;
            emitter.emit(x142) // yield
            val x143 = ()
          }
        }
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x148
    val x161 = x148.parallelDo(new DoFn[java.lang.String, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {

        val x152 = input // loop var x150;
        val x153 = x149(x152);
        val x158 = if (x153) {
          emitter.emit(x152) // yield
          val x154 = ()
          x154
        } else {
          val x156 = () // skip;
          x156
        }
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x161
    val x174 = x161.parallelDo(new DoFn[java.lang.String, java.lang.String, Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {

        val x165 = input // loop var x163;
        val x166 = x162(x165);
        val x171 = if (x166) {
          emitter.emit(x165) // yield
          val x167 = ()
          x167
        } else {
          val x169 = () // skip;
          x169
        }
      }

      def cleanup(emitter: Emitter[java.lang.String]): Unit = {}
    })

    // x174
    val x183 = x174.parallelDo(new DoFn[java.lang.String, scala.Tuple2[java.lang.String, Int], Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {

        val x178 = input // loop var x176;
        val x179 = x175(x178);
        emitter.emit(x179) // yield
        val x180 = ()
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {}
    })

    val x184 = x183.groupByKey;
    @inline
    def x185(x49: Int, x50: Int) = {
      val x51 = x49 + x50;
      x51: Int
    }
    val x186 = x184.combine(x185);
    val x187 = persist(TextOutput.toTextFile(x186, x54));
  }
}
// Types that are used in this program

case class WikiArticle(var pageId: Long = 0L, var name: java.lang.String = " ", var updated: ch.epfl.distributed.datastruct.Date = new ch.epfl.distributed.datastruct.Date(), var xml: java.lang.String = " ", var plaintext: java.lang.String = " ") extends Writable {
  def this() = this(pageId = 0L)

  var __bitset: Long = 31;
  override def readFields(in: DataInput) {
    __bitset = WritableUtils.readVLong(in)
    __bitset match {
      case 31 => readFields_0_1_2_3_4(in)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }
  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, __bitset)
    __bitset match {
      case 31 => write_0_1_2_3_4(out)
      case x => throw new RuntimeException("Unforeseen bit combination " + x)
    }
  }

  def readFields_0_1_2_3_4(in: DataInput) {
    pageId = WritableUtils.readVLong(in)
    name = in.readUTF
    updated.readFields(in)
    xml = in.readUTF
    plaintext = in.readUTF
  }
  def write_0_1_2_3_4(out: DataOutput) {
    WritableUtils.writeVLong(out, pageId)
    out.writeUTF(name)
    updated.write(out)
    out.writeUTF(xml)
    out.writeUTF(plaintext)
  }

}

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
