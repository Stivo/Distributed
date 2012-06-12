/**
 * ***************************************
 * Emitting Scoobi Code
 * *****************************************
 */

package dcdsl.generated.v1;
import ch.epfl.distributed.utils.WireFormatsGen
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat
import ch.epfl.distributed.datastruct._
import ch.epfl.distributed.utils._
import org.apache.hadoop.io.{ Writable, WritableUtils }
import java.io.{ DataInput, DataOutput }
object WordCountApp extends ScoobiApp {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
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
    val x2 = x1(0);
    val x3 = TextInput.fromTextFile(x2);
    // x3
    val x1694 = x3.parallelDo(new DoFn[java.lang.String, scala.Tuple2[java.lang.String, Int], Unit] {
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {

        val x221 = input // loop var x219;
        val x314 = x221.split("""	""", 5);
        val x315 = x314(4);
        val x1409 = """\n""" + x315;
        val x1593 = x1409.replaceAll("""\[\[.*?\]\]""", """ """);
        val x1594 = x1593.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
        val x1669 = x1594.split("""[^a-zA-Z0-9']+""", 0);
        val x1670 = x1669.toSeq;
        // x1670
        {
          val it = x1670.iterator
          while (it.hasNext) { // flatMap
            val input = it.next()
            val x1672 = input // loop var x260;
            val x1673 = x1672.length;
            val x1674 = x1673 > 1;
            val x1686 = if (x1674) {
              val x1675 = x1672.matches("""(thumb|left|right|\d+px){2,}""");
              val x1676 = !x1675;
              val x1682 = if (x1676) {
                val x1677 = (x1672, 1);
                emitter.emit(x1677) // yield
                val x1678 = ()
                x1678
              } else {
                val x1680 = () // skip;
                x1680
              }
              x1682
            } else {
              val x1684 = () // skip;
              x1684
            }
          }
        }
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {}
    })

    val x1695 = x1694.groupByKey;
    @inline
    def x1394(x49: Int, x50: Int) = {
      val x51 = x49 + x50;
      x51: Int
    }
    val x1696 = x1695.combine(x1394);
    val x1697 = persist(TextOutput.toTextFile(x1696, x54));
  }
}
// Types that are used in this program

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
