package ch.epfl.distributed

import scala.virtualization.lms.common.{ ScalaGenBase, LoopsFatExp, ScalaGenLoopsFat }
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter
import scala.virtualization.lms.common.WorklistTransformer
import java.io.FileWriter

trait CrunchGenDList extends ScalaGenBase
    with DListFieldAnalysis
    with DListTransformations with Matchers with FastWritableTypeFactory {
  /*
 * TODO:
 * - More PTypes, cleaner tuple handling etc
 * Maybe:
 * - kryo instead of writable. Makes join easier and other stuff too.
 * - implement inline closures
 * 
 * Unsupported:
 * - Usage of vars: collection name changes
 * Idea:
 * Implement a PType for KryoFormat => BytesWritable
 * Same kryo scheme as in scoobi
 * one PType that converts from Kryo to BytesWritable
 * use that instead of generating Writables
 *  
 * => Advantage:
 * Kryo serialization works, Writable
 */
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListJoin,
    DListReduce,
    ComputationNode,
    DListNode,
    GetArgs,
    IteratorValue
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  val getProjectName = "crunch"

  var wireFormats = List[String]()

  override def remap[A](x: Manifest[A]) = {
    var out = super.remap(x)
    out = out.replaceAll("Int(?!e)", "java.lang.Integer")
    out.replaceAll("scala.Tuple2", "CPair")
  }

  def createPType(x: Manifest[_], topLevel: Boolean = true): String = {
    if (x.toString.startsWith("scala.Tuple2")) {
      val p1 = createPType(x.typeArguments(0), false)
      val p2 = createPType(x.typeArguments(1), false)
      if (topLevel)
        return "Writables.tableOf(%s, %s)".format(p1, p2)
      else
        return "Writables.pairs(%s, %s)".format(p1, p2)
    }
    val cleaned = remap(x)
    cleaned match {
      case "java.lang.String" => return "Writables.strings()"
      case "java.lang.Integer" => return "Writables.ints()"
      case _ =>
    }
    if (typeHandler.typeInfos2.contains(cleaned)) {
      return "Writables.records(classOf[%s])".format(cleaned);
    }
    return "TODO PType for " + x
  }
  def createParallelDo(listIn: Exp[_], cn: ComputationNode, body: String): String = {
    createParallelDo(listIn, cn.getElementTypes._1, cn.getElementTypes._2, body)
  }

  def createParallelDo(listIn: Exp[_], mIn: Manifest[_], mOut: Manifest[_], body: String): String = {
    """%5$s.parallelDo(new DoFn[%1$s, %2$s] {
      def process(input: %1$s, emitter: Emitter[%2$s]): Unit = {
    	  %4$s
      }
    }, %3$s)""".format(remap(mIn), remap(mOut), createPType(mOut), body, quote(listIn))
  }
  def createParallelDoProlog(listIn: Exp[_], mIn: Manifest[_], mOut: Manifest[_]): String = {
    """%5$s.parallelDo(new DoFn[%1$s, %2$s] {
      def process(input: %1$s, emitter: Emitter[%2$s]): Unit = {
    	  """.format(remap(mIn), remap(mOut), createPType(mOut), "", quote(listIn))
  }
  def createParallelDoEpilog(mOut: Manifest[_]): String = {
    """
    }, %s)""".format(createPType(mOut))
  }

  def castPrimitive(s: Exp[_]) = {
    if (remap(s.tp) == "java.lang.Integer")
      quote(s) + ".asInstanceOf[java.lang.Integer]"
    else
      quote(s)
  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "CPair.of(%s, %s)".format(castPrimitive(elems("_1")), castPrimitive(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.Field(tuple, x, tp) if (tuple.tp.toString.startsWith("scala.Tuple2")) => emitValDef(sym, "%s.%s".format(quote(tuple), if (x == "_1") "first()" else "second()"))
      case IR.Field(tuple, x, tp) if (x == "_1" || x == "_2") => emitValDef(sym, "%s.%s // TODO This is a hack, the symbol for %s should have a tuple type instead of %s".format(quote(tuple), if (x == "_1") "first()" else "second()", Def.unapply(tuple), tp))
      case nv @ NewDList(filename) => emitValDef(sym, "pipeline.readTextFile(%s)".format(quote(filename)))
      case vs @ DListSave(dlist, filename) => emitValDef(sym, "pipeline.writeTextFile(%s, %s)".format(quote(dlist), quote(filename)))
      case vm @ DListMap(dlist, function) => {
        // TODO
        emitValDef(sym, createParallelDo(dlist, vm, "emitter.emit(%s(input))".format(handleClosure(vm.closure))))
      }
      case vm @ DListFilter(dlist, function) =>
        emitValDef(sym, createParallelDo(dlist, vm, "if (%s(input)) \n emitter.emit(input)".format(handleClosure(vm.closure))))

      case vm @ DListFlatMap(dlist, function) =>
        emitValDef(sym, createParallelDo(dlist, vm, "%s(input).foreach(emitter.emit)".format(handleClosure(vm.closure))))
      case vm @ DListFlatten(v1) => {
        var out = v1.map(quote(_)).mkString("(", ").union(", ")")
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist) => emitValDef(sym, "%s.groupByKey".format(quote(dlist)))
      case v @ DListJoin(left, right) => {
        // create tagged value subclass
        if (typeHandler.remappings.contains(v.mV1) && typeHandler.remappings.contains(v.mV2)) {
          val tv = """class TaggedValue_%1$s_%2$s(left: Boolean, v1: %1$s, v2: %2$s) extends TaggedValue[%1$s, %2$s](left, v1, v2) {
    	   def this() = this(false, new %1$s(), new %2$s())
        }""".format(remap(v.mV1), remap(v.mV2))
          val tvname = "TaggedValue_%1$s_%2$s".format(remap(v.mV1), remap(v.mV2))
          types += tvname -> tv
          //emitValDef(sym, "joinWritables(classOf[%s], %s, %s)".format(tvname, quote(left), quote(right)))
          emitValDef(sym, "joinNotNull(%s, %s)".format(quote(left), quote(right)))
        } else {
          emitValDef(sym, "join(%s, %s)".format(quote(left), quote(right)))
        }
      }
      case red @ DListReduce(dlist, f) => emitValDef(sym,
        "%s.combineValues(new CombineWrapper(%s))".format(quote(dlist), handleClosure(f)))
      case sd @ IteratorValue(r, i) => emitValDef(sym, "input // loop var " + quote(i))
      case GetArgs() => emitValDef(sym, "args.drop(1)")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }
  val collectionName = "PCollection"

  def transformTree[B: Manifest](block: Block[B]) = {
    var y = block
    // narrow existing maps
    y = doNarrowExistingMaps(y)
    // inserting narrower maps and narrow
    y = insertNarrowersAndNarrow(y, new NarrowerInsertionTransformation)

    prepareGraphData(y, true)

    if (loopFusion) {
      y = new MonadicToLoopsTransformation().run(y)
      if (inlineInLoopFusion)
        y = new InlineTransformation().run(y)
    }
    y
  }

  override def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, streamIn: PrintWriter, pack: String)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    // not implemented yet, just for easier reading anyway
    inlineClosures = false
    val capture = new StringWriter
    val stream = new PrintWriter(capture)

    stream.println("/*****************************************\n" +
      "  Emitting Crunch Code                  \n" +
      "*******************************************/")
    stream.println("""package dcdsl.generated%2$s;

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

object %1$s {
  def main(args: Array[String]) {
    val newArgs = (List("asdf") ++ args.toList).toArray
    ToolRunner.run(new Configuration(), new %1$s(), newArgs);
  }
}
        
class %1$s extends Configured with Tool with Serializable {
    %3$s
    def run(args: Array[String]): Int = {
  		val pipeline = new MRPipeline(classOf[%1$s], getConf());
        """.format(className, makePackageName(pack), getOptimizations()))

    val x = fresh[A]
    var y = reifyBlock(f(x))
    typeHandler = new TypeHandler(y)

    y = transformTree(y)

    withStream(stream) {
      emitBlock(y)
    }
    stream.println("""
    pipeline.done();
    return 0;
  }
}""")
    stream.println("// Types that are used in this program")
    stream.println(restTypes.values.toList.sorted.mkString("\n"))

    val prevTypes = types.keySet.filter(_ => true)
    addTypes()
    stream.print(types.filterKeys(x => !prevTypes.contains(x)).values.mkString("\n"))

    stream.println("/*****************************************\n" +
      "  End of Crunch Code                  \n" +
      "*******************************************/")

    stream.flush

    val out = capture.toString
    //val newOut = out.replace("###wireFormats###", mkWireFormats)
    streamIn.print(out)
    reset
    Nil
  }

  def addTypes() {}

}

trait KryoCrunchGenDList extends CrunchGenDList with CaseClassTypeFactory {
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    DListJoin
  }

  override val fastWritableTypeFactoryEnabled = false

  override def makeTypeFor(name: String, fields: Iterable[String]): String = {
    super[CaseClassTypeFactory].makeTypeFor(name, fields)
  }

  override def getSuperTraitsForTrait: String = "extends KryoFormat"

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case v @ DListJoin(left, right) => {
        emitValDef(sym, "joinNotNull(%s, %s)".format(quote(left), quote(right)))
      }
      case _ => super.emitNode(sym, rhs)
    }
  }

  override def castPrimitive(s: Exp[_]) = {
    val cleaned = remap(s.tp)
    if (typeHandler.typeInfos2.contains(cleaned)) {
      quote(s) + ".asInstanceOf[%s]".format(cleaned)
    } else {
      super.castPrimitive(s)
    }
  }

  override def createPType(x: Manifest[_], topLevel: Boolean = true): String = {
    val cleaned = remap(x)
    if (typeHandler.typeInfos2.contains(cleaned)) {
      return "KryoWritables.make[%s]".format(cleaned);
    }
    return super.createPType(x, topLevel)
  }

  override def addTypes() {
    types += "Registrator" -> """class Registrator extends KryoRegistrator {
        import com.esotericsoftware.kryo.Kryo
        def registerClasses(kryo: Kryo) {
        %s
    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}""".format(types.keys.toList.sorted.map("kryo.register(classOf[" + _ + "])").mkString("\n"))
    types += "KryoInstance" -> """object KryoInstance {
  def apply() = {
    val ks = new KryoSerializer()
    val r = new Registrator()
    r.registerClasses(ks.kryo)
    ks.newInstance
  }
}"""
    types += "KryoWritables" -> """
//import com.cloudera.crunch.types.writable.KryoWritableType
import com.cloudera.crunch.`type`.writable.KryoWritableType
import org.apache.hadoop.io.BytesWritable

object KryoWritables {
  def make[T <: KryoFormat: Manifest] = {
    type W = BytesWritable
    val in = new KryoInputFn[T].asInstanceOf[MapFn[W, T]]
    val out = new KryoOutputFn[T].asInstanceOf[MapFn[T, W]]
    new KryoWritableType[T, BytesWritable](in, out)
  }
}

class KryoInputFn[T <: KryoFormat: Manifest] extends MapFn[BytesWritable, T] {
  val m = manifest[T]
  lazy val si = KryoInstance()
  def map(in: BytesWritable) = {
    m.erasure.cast(si.deserialize(in.get)).asInstanceOf[T]
  }
}
class KryoOutputFn[T <: KryoFormat] extends MapFn[T, BytesWritable] {
  lazy val si = KryoInstance()
  def map(in: T) = {
    new BytesWritable(si.serialize(in))
  }
}
"""
  }
}

trait ScalaGenCrunchFat extends ScalaGenLoopsFat with CrunchGenDList {
  val IR: DListOpsExp with LoopsFatExp
  import IR._

  override def emitFatNode(sym: List[Sym[Any]], rhs: FatDef) = rhs match {
    case SimpleFatLoop(Def(ShapeDep(sd)), x, rhs) =>
      val ii = x
      var outType: Manifest[_] = null
      for ((l, r) <- sym zip rhs) r match {
        case IteratorCollect(g, Block(y)) =>
          outType = g.tp.typeArguments.head
          stream.println("val " + quote(sym.head) + " = " + createParallelDoProlog(sd, sd.tp.typeArguments(0), outType))
        case ForeachElem(y) =>
          stream.println("{ val it = " + quote(sd) + ".iterator") // hack for the wrong interface
          stream.println("while(it.hasNext) { // flatMap")
          stream.println("val input = it.next()")
      }

      val gens = for ((l, r) <- (sym zip rhs) if !r.isInstanceOf[ForeachElem[_]]) yield r match {
        case IteratorCollect(g, Block(y)) =>
          (g, (s: List[String]) => {
            stream.println("emitter.emit(" + s.head + ")// yield")
            stream.println("val " + quote(g) + " = ()")
          })
      }

      withGens(gens) {
        emitFatBlock(syms(rhs).map(Block(_)))
      }

      stream.println("}")

      // with iterators there is no horizontal fusion so we do not have to worry about the ugly prefix and suffix
      for ((l, r) <- (sym zip rhs)) r match {
        case IteratorCollect(g, Block(y)) =>
          stream.println(createParallelDoEpilog(outType))
        case ForeachElem(y) =>
          stream.println("}")
      }
    case _ => super.emitFatNode(sym, rhs)
  }
}

trait CrunchGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with CrunchGenDList with ScalaGenCrunchFat

trait KryoCrunchGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with KryoCrunchGenDList with ScalaGenCrunchFat
