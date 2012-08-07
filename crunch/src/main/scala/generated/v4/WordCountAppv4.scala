/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v4;

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
  // regex patterns pre compiled: true
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[WordCountApp], getConf());

    val x1 = args.drop(1);
    val x61 = x1(1);
    val x53 = x1(2);
    val x54 = x53.toInt;
    val x2 = x1(0);
    val x3 = pipeline.readTextFile(x2);
    // x3
    val x5 = new ch.epfl.distributed.datastruct.RegexFrontend("""	""", true, true);
    val x23 = new ch.epfl.distributed.datastruct.RegexFrontend("""\[\[.*?\]\]""", true, true);
    val x28 = new ch.epfl.distributed.datastruct.RegexFrontend("""(\\[ntT]|\.)\s*(thumb|left|right)*""", true, true);
    val x33 = new ch.epfl.distributed.datastruct.RegexFrontend("""[^a-zA-Z0-9']+""", true, true);
    val x44 = new ch.epfl.distributed.datastruct.RegexFrontend("""(thumb|left|right|\d+px){2,}""", true, true);
    val x1701 = x3.parallelDo(new DoFn[java.lang.String, CPair[java.lang.String, java.lang.Integer]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.String, java.lang.Integer]]): Unit = {

        val x228 = input // loop var x226;
        val x321 = x5.split(x228, 5);
        val x322 = x321(4);
        val x1416 = """\n""" + x322;
        val x1600 = x23.replaceAll(x1416, """ """);
        val x1601 = x28.replaceAll(x1600, """ """);
        val x1676 = x33.split(x1601, 0);
        val x1677 = x1676.toSeq;
        // x1677
        {
          val it = x1677.iterator
          while (it.hasNext) { // flatMap
            val input = it.next()
            val x1679 = input // loop var x267;
            val x1680 = x1679.length;
            val x1681 = x1680 > 1;
            val x1693 = if (x1681) {
              val x1682 = x44.matches(x1679);
              val x1683 = !x1682;
              val x1689 = if (x1683) {
                val x1684 = CPair.of(x1679, 1.asInstanceOf[java.lang.Integer]);
                emitter.emit(x1684) // yield
                val x1685 = ()
                x1685
              } else {
                val x1687 = () // skip;
                x1687
              }
              x1689
            } else {
              val x1691 = () // skip;
              x1691
            }
          }
        }
      }

    }, Writables.tableOf(Writables.strings(), Writables.ints()))
    val x1702 = x1701.groupByKey(x54);
    @inline
    def x1401(x56: java.lang.Integer, x57: java.lang.Integer) = {
      val x58 = x56 + x57;
      x58: java.lang.Integer
    }
    val x1703 = x1702.combineValues(new CombineWrapper(x1401));
    val x1704 = pipeline.writeTextFile(x1703, x61);

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
