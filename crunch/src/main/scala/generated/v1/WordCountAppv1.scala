/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v1;

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
    val x56 = x1(1);
    val x48 = x1(2);
    val x49 = x48.toInt;
    val x2 = x1(0);
    val x3 = pipeline.readTextFile(x2);
    // x3
    val x1696 = x3.parallelDo(new DoFn[java.lang.String, CPair[java.lang.String, java.lang.Integer]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.String, java.lang.Integer]]): Unit = {

        val x223 = input // loop var x221;
        val x316 = x223.split("""	""", 5);
        val x317 = x316(4);
        val x1411 = """\n""" + x317;
        val x1595 = x1411.replaceAll("""\[\[.*?\]\]""", """ """);
        val x1596 = x1595.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
        val x1671 = x1596.split("""[^a-zA-Z0-9']+""", 0);
        val x1672 = x1671.toSeq;
        // x1672
        {
          val it = x1672.iterator
          while (it.hasNext) { // flatMap
            val input = it.next()
            val x1674 = input // loop var x262;
            val x1675 = x1674.length;
            val x1676 = x1675 > 1;
            val x1688 = if (x1676) {
              val x1677 = x1674.matches("""(thumb|left|right|\d+px){2,}""");
              val x1678 = !x1677;
              val x1684 = if (x1678) {
                val x1679 = CPair.of(x1674, 1.asInstanceOf[java.lang.Integer]);
                emitter.emit(x1679) // yield
                val x1680 = ()
                x1680
              } else {
                val x1682 = () // skip;
                x1682
              }
              x1684
            } else {
              val x1686 = () // skip;
              x1686
            }
          }
        }
      }

    }, Writables.tableOf(Writables.strings(), Writables.ints()))
    val x1697 = x1696.groupByKey(x49);
    @inline
    def x1396(x51: java.lang.Integer, x52: java.lang.Integer) = {
      val x53 = x51 + x52;
      x53: java.lang.Integer
    }
    val x1698 = x1697.combineValues(new CombineWrapper(x1396));
    val x1699 = pipeline.writeTextFile(x1698, x56);

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
