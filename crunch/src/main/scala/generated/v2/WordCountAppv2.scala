/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v2;

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
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[WordCountApp], getConf());

    val x1 = args.drop(1);
    val x59 = x1(1);
    val x2 = x1(0);
    val x3 = pipeline.readTextFile(x2);
    // x3
    val x5 = new ch.epfl.distributed.datastruct.RegexFrontend("""	""", false, false);
    val x23 = new ch.epfl.distributed.datastruct.RegexFrontend("""\[\[.*?\]\]""", false, false);
    val x28 = new ch.epfl.distributed.datastruct.RegexFrontend("""(\\[ntT]|\.)\s*(thumb|left|right)*""", false, false);
    val x33 = new ch.epfl.distributed.datastruct.RegexFrontend("""[^a-zA-Z0-9']+""", false, false);
    val x44 = new ch.epfl.distributed.datastruct.RegexFrontend("""(thumb|left|right|\d+px){2,}""", false, false);
    val x1699 = x3.parallelDo(new DoFn[java.lang.String, CPair[java.lang.String, java.lang.Integer]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.String, java.lang.Integer]]): Unit = {

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
                val x1682 = CPair.of(x1677, 1.asInstanceOf[java.lang.Integer]);
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

    }, Writables.tableOf(Writables.strings(), Writables.ints()))
    val x1700 = x1699.groupByKey;
    @inline
    def x1399(x54: java.lang.Integer, x55: java.lang.Integer) = {
      val x56 = x54 + x55;
      x56: java.lang.Integer
    }
    val x1701 = x1700.combineValues(new CombineWrapper(x1399));
    val x1702 = pipeline.writeTextFile(x1701, x59);

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
