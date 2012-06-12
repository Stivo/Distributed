/**
 * ***************************************
 * Emitting Crunch Code
 * *****************************************
 */
package dcdsl.generated.v0;

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
  // field reduction: false
  // loop fusion: false
  // inline in loop fusion: false
  // regex patterns pre compiled: false
  def run(args: Array[String]): Int = {
    val pipeline = new MRPipeline(classOf[WordCountApp], getConf());

    val x1 = args.drop(1);
    val x2 = x1(0);
    val x3 = pipeline.readTextFile(x2);
    @inline
    def x14(x4: (java.lang.String)) = {
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
    val x15 = x3.parallelDo(new DoFn[java.lang.String, WikiArticle] {
      def process(input: java.lang.String, emitter: Emitter[WikiArticle]): Unit = {
        emitter.emit(x14(input))
      }
    }, Writables.records(classOf[WikiArticle]));
    @inline
    def x19(x16: (WikiArticle)) = {
      val x17 = x16.plaintext;
      val x18 = """\n""" + x17;
      x18: java.lang.String
    }
    val x20 = x15.parallelDo(new DoFn[WikiArticle, java.lang.String] {
      def process(input: WikiArticle, emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x19(input))
      }
    }, Writables.strings());
    @inline
    def x23(x21: (java.lang.String)) = {
      val x22 = x21.replaceAll("""\[\[.*?\]\]""", """ """);
      x22: java.lang.String
    }
    val x24 = x20.parallelDo(new DoFn[java.lang.String, java.lang.String] {
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x23(input))
      }
    }, Writables.strings());
    @inline
    def x27(x25: (java.lang.String)) = {
      val x26 = x25.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", """ """);
      x26: java.lang.String
    }
    val x28 = x24.parallelDo(new DoFn[java.lang.String, java.lang.String] {
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {
        emitter.emit(x27(input))
      }
    }, Writables.strings());
    @inline
    def x32(x29: (java.lang.String)) = {
      val x30 = x29.split("""[^a-zA-Z0-9']+""", 0);
      val x31 = x30.toSeq;
      x31: scala.collection.Seq[java.lang.String]
    }
    val x33 = x28.parallelDo(new DoFn[java.lang.String, java.lang.String] {
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {
        x32(input).foreach(emitter.emit)
      }
    }, Writables.strings());
    @inline
    def x37(x34: (java.lang.String)) = {
      val x35 = x34.length;
      val x36 = x35 > 1;
      x36: Boolean
    }
    val x38 = x33.parallelDo(new DoFn[java.lang.String, java.lang.String] {
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {
        if (x37(input))
          emitter.emit(input)
      }
    }, Writables.strings());
    @inline
    def x42(x39: (java.lang.String)) = {
      val x40 = x39.matches("""(thumb|left|right|\d+px){2,}""");
      val x41 = !x40;
      x41: Boolean
    }
    val x43 = x38.parallelDo(new DoFn[java.lang.String, java.lang.String] {
      def process(input: java.lang.String, emitter: Emitter[java.lang.String]): Unit = {
        if (x42(input))
          emitter.emit(input)
      }
    }, Writables.strings());
    @inline
    def x46(x44: (java.lang.String)) = {
      val x45 = CPair.of(x44, 1.asInstanceOf[java.lang.Integer]);
      x45: CPair[java.lang.String, java.lang.Integer]
    }
    val x47 = x43.parallelDo(new DoFn[java.lang.String, CPair[java.lang.String, java.lang.Integer]] {
      def process(input: java.lang.String, emitter: Emitter[CPair[java.lang.String, java.lang.Integer]]): Unit = {
        emitter.emit(x46(input))
      }
    }, Writables.tableOf(Writables.strings(), Writables.ints()));
    val x48 = x47.groupByKey;
    @inline
    def x52(x49: java.lang.Integer, x50: java.lang.Integer) = {
      val x51 = x49 + x50;
      x51: java.lang.Integer
    }
    val x53 = x48.combineValues(new CombineWrapper(x52));
    val x54 = x1(1);
    val x55 = pipeline.writeTextFile(x53, x54);

    pipeline.done();
    return 0;
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
 * End of Crunch Code
 * *****************************************
 */
