/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v1;

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

object WordCountApp {
  def main(args: Array[String]) {
    val newArgs = (List("asdf") ++ args.toList).toArray
    ToolRunner.run(new Configuration(), new WordCountApp(), newArgs);
  }
}

class WordCountApp extends Configured with Tool with Serializable {
  // field reduction: true
  // loop fusion: true
  // inline in loop fusion: true
  // regex patterns pre compiled: false
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[WordCountApp], getConf());

    val x1 = args.drop(1);
    val x54 = x1(1);
    val x2 = x1(0);
    val x3 = pipeline.readTextFile(x2);
    // x3
    val x1694 = x3.parallelDo(new DoFn[java.lang.String, CPair[java.lang.String, java.lang.Integer]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.String, java.lang.Integer]]): Unit = {

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
                val x1677 = CPair.of(x1672, 1.asInstanceOf[java.lang.Integer]);
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

    }, Writables.tableOf(Writables.strings(), Writables.ints()))
    val x1695 = x1694.groupByKey;
    @inline
    def x1394(x49: java.lang.Integer, x50: java.lang.Integer) = {
      val x51 = x49 + x50;
      x51: java.lang.Integer
    }
    val x1696 = x1695.combineValues(new CombineWrapper(x1394));
    val x1697 = pipeline.writeTextFile(x1696, x54);

    pipeline.done();
    return 0;
  }
}
// Types that are used in this program

/**
 * ***************************************
 * End of Crunch Code
 * *****************************************
 */
