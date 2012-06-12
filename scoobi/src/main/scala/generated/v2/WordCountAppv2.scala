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
object WordCountApp extends ScoobiApp {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: true
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
    val x59 = x1(1);
    val x2 = x1(0);
    val x3 = TextInput.fromTextFile(x2);
    // x3
    val x1699 = x3.parallelDo(new DoFn[java.lang.String, scala.Tuple2[java.lang.String, Int], Unit] {
      lazy val x5 = new ch.epfl.distributed.datastruct.RegexFrontend("""	""", false, false);
      lazy val x23 = new ch.epfl.distributed.datastruct.RegexFrontend("""\[\[.*?\]\]""", false, false);
      lazy val x28 = new ch.epfl.distributed.datastruct.RegexFrontend("""(\\[ntT]|\.)\s*(thumb|left|right)*""", false, false);
      lazy val x33 = new ch.epfl.distributed.datastruct.RegexFrontend("""[^a-zA-Z0-9']+""", false, false);
      lazy val x44 = new ch.epfl.distributed.datastruct.RegexFrontend("""(thumb|left|right|\d+px){2,}""", false, false);
      def setup(): Unit = {}
      def process(input: java.lang.String, emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {

        val x226 = input // loop var x224;
        val x319 = x5.split(x226, 5);
        val x320 = x319(4);
        val x1414 = """\n""" + x320;
        val x1598 = x23.replaceAll(x1414, """ """);
        val x1599 = x28.replaceAll(x1598, """ """);
        val x1674 = x33.split(x1599, 0);
        val x1675 = x1674.toSeq;
        // x1675
        {
          val it = x1675.iterator
          while (it.hasNext) { // flatMap
            val input = it.next()
            val x1677 = input // loop var x265;
            val x1678 = x1677.length;
            val x1679 = x1678 > 1;
            val x1691 = if (x1679) {
              val x1680 = x44.matches(x1677);
              val x1681 = !x1680;
              val x1687 = if (x1681) {
                val x1682 = (x1677, 1);
                emitter.emit(x1682) // yield
                val x1683 = ()
                x1683
              } else {
                val x1685 = () // skip;
                x1685
              }
              x1687
            } else {
              val x1689 = () // skip;
              x1689
            }
          }
        }
      }

      def cleanup(emitter: Emitter[scala.Tuple2[java.lang.String, Int]]): Unit = {}
    })

    val x1700 = x1699.groupByKey;
    @inline
    def x1399(x54: Int, x55: Int) = {
      val x56 = x54 + x55;
      x56: Int
    }
    val x1701 = x1700.combine(x1399);
    val x1702 = persist(TextOutput.toTextFile(x1701, x59));
  }
}
// Types that are used in this program

/**
 * ***************************************
 * End of Scoobi Code
 * *****************************************
 */
