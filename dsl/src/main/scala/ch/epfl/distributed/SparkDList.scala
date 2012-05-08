package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter

trait SparkProgram extends DListOpsExp with DListImplOps with SparkDListOpsExp

trait SparkDListOps extends DListOps {
  implicit def repVecToSparkVecOps[A: Manifest](dlist: Rep[DList[A]]) = new dlistSparkOpsCls(dlist)
  class dlistSparkOpsCls[A: Manifest](dlist: Rep[DList[A]]) {
    def cache() = dlist_cache(dlist)
    def collect() = dlist_collect(dlist)
    def takeSample(withReplacement: Rep[Boolean], num: Rep[Int], seed: Rep[Int]) = dlist_takeSample(dlist, withReplacement, num, seed)
  }

  def dlist_cache[A: Manifest](dlist: Rep[DList[A]]): Rep[DList[A]]
  def dlist_collect[A: Manifest](dlist: Rep[DList[A]]): Rep[Iterable[A]]
  def dlist_takeSample[A: Manifest](dlist: Rep[DList[A]], withReplacement: Rep[Boolean], num: Rep[Int], seed: Rep[Int]): Rep[Iterable[A]]
}

trait SparkDListOpsExp extends DListOpsExp with SparkDListOps {
  case class DListReduceByKey[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]], closure: Exp[(V, V) => V])
      extends Def[DList[(K, V)]] with Closure2Node[V, V, V]
      with PreservingTypeComputation[DList[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getType = manifest[DList[(K, V)]]
  }

  case class DListCache[A: Manifest](in: Exp[DList[A]]) extends Def[DList[A]] with PreservingTypeComputation[DList[A]] {
    val mA = manifest[A]
    def getType = manifest[DList[A]]
  }

  case class DListCollect[A: Manifest](in: Exp[DList[A]]) extends Def[Iterable[A]] with DListNode {
    val mA = manifest[A]
    def getType = manifest[DList[A]]
  }

  case class DListTakeSample[A: Manifest](dlist: Exp[DList[A]], withReplacement: Exp[Boolean], num: Exp[Int], seed: Exp[Int]) extends Def[Iterable[A]] with DListNode {
    val mA = manifest[A]
  }

  override def dlist_cache[A: Manifest](in: Rep[DList[A]]) = DListCache[A](in)

  override def dlist_collect[A: Manifest](dlist: Exp[DList[A]]) = DListCollect[A](dlist)

  override def dlist_takeSample[A: Manifest](dlist: Exp[DList[A]], withReplacement: Exp[Boolean], num: Exp[Int], seed: Exp[Int]) = DListTakeSample(dlist, withReplacement, num, seed)

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = {
    val out = (e match {
      case v @ DListReduceByKey(dlist, func) => DListReduceByKey(f(dlist), f(func))(v.mKey, v.mValue)
      case v @ DListCache(in) => DListCache(f(in))(v.mA)
      case v @ DListCollect(in) => DListCollect(f(in))(v.mA)
      case v @ DListTakeSample(in, r, n, s) => DListTakeSample(f(in), f(r), f(n), f(s))(v.mA)
      case _ => super.mirrorDef(e, f)
    })
    copyMetaInfo(e, out)
    out.asInstanceOf[Def[A]]
  }

}

trait SparkTransformations extends DListTransformations {
  val IR: DListOpsExp with SparkDListOpsExp
  import IR.{ DListReduceByKey, DListReduce, DListGroupByKey, DListMap }
  import IR.{ Def, Exp }

  class ReduceByKeyTransformation extends TransformationRunner {
    import wt.IR._
    def registerTransformations(analyzer: Analyzer) {
      val reduces = analyzer.nodes.flatMap {
        case d @ DListReduce(r, f) => Some(d)
        case _ => None
      }
      reduces.foreach {
        case d @ DListReduce(Def(DListGroupByKey(r)), f) =>
          val stm = findDefinition(d).get
          val newReducer = new DListReduceByKey(wt(r), wt(f))(d.mKey, d.mValue)
          val atom = toAtom2(newReducer)(mtype(stm.syms.head.tp), implicitly[SourceContext])
          System.out.println("Registering " + stm + " to " + newReducer)
          wt.register(stm.syms.head)(atom)
        case _ =>
      }
    }
  }

}

trait SparkDListFieldAnalysis extends DListFieldAnalysis {
  val IR: SparkDListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListReduce,
    ComputationNode,
    DListNode,
    DListReduceByKey,
    DListCache,
    DListCollect,
    DListTakeSample,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def newFieldAnalyzer(block: Block[_], typeHandlerForUse: TypeHandler = typeHandler) = new SparkFieldAnalyzer(block, typeHandlerForUse)

  class SparkNarrowerInsertionTransformation extends NarrowerInsertionTransformation {
    override def registerTransformations(analyzer: Analyzer) {
      super.registerTransformations(analyzer)
      analyzer.narrowBefore.foreach {
        case d @ DListReduceByKey(in, func) =>
          val stm = findDefinition(d).get
          class ReduceByKeyTransformer[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]]) {
            val mapNew = makeNarrower(in)
            val redNew = DListReduceByKey(mapNew, wt(func))
            wt.register(stm.syms.head)(IR.toAtom2(redNew)(IR.mtype(d.getTypes._2), implicitly[SourceContext]))
          }
          new ReduceByKeyTransformer(d.in)(d.mKey, d.mValue)

        case d @ DListCache(dlist) =>
          val stm = findDefinition(d).get
          val mapNew = makeNarrower(dlist)
          val cacheNew = IR.dlist_cache(mapNew)(d.mA)
          wt.register(stm.syms.head)(cacheNew)
        case _ =>
      }

    }
  }

  class SparkFieldAnalyzer(block: Block[_], typeHandler: TypeHandler) extends FieldAnalyzer(block, typeHandler) {

    override def isNarrowBeforeCandidate(x: DListNode) = x match {
      case DListReduceByKey(_, _) => true
      case DListCache(_) => true
      case x => super.isNarrowBeforeCandidate(x)
    }

    override def computeFieldReads(node: DListNode): Set[FieldRead] = node match {
      case v @ DListReduceByKey(in, func) => {
        // analyze function
        // convert the analyzed accesses to accesses of input._2
        val part1 = (analyzeFunction(v) ++ Set(FieldRead("input")))
          .map(_.path.drop(5))
          .map(x => "input._2" + x)
          .map(FieldRead)
        // add the accesses from successors
        val part2 = v.successorFieldReads
        val part3 = visitAll("input._1", v.getTypes._1.typeArguments(0))
        (part1 ++ part2 ++ part3).toSet
      }

      case v @ DListCollect(in) => visitAll("input", v.mA)
    		  
      case v @ DListTakeSample(in,_,_,_) => visitAll("input", v.mA)

      case v @ DListCache(in) => node.successorFieldReads.toSet

      case _ => super.computeFieldReads(node)
    }
  }

}

trait SparkGenDList extends ScalaGenBase with ScalaGenDList with DListTransformations
    with SparkTransformations with Matchers with SparkDListFieldAnalysis with CaseClassTypeFactory {

  val IR: SparkDListOpsExp
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
    DListCollect,
    ComputationNode,
    DListNode,
    DListTakeSample,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ DListReduceByKey, DListCache }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case nv @ NewDList(filename) => emitValDef(sym, "sc.textFile(%s)".format(quote(filename)))
      case vs @ DListSave(dlist, filename) => stream.println("%s.saveAsTextFile(%s)".format(quote(dlist), quote(filename)))
      case vm @ DListMap(dlist, function) => emitValDef(sym, "%s.map(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFilter(dlist, function) => emitValDef(sym, "%s.filter(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatten(v1) => {
        var out = v1.map(quote(_)).mkString("(", ").union(", ")")
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist) => emitValDef(sym, "%s.groupByKey".format(quote(dlist)))
      case v @ DListJoin(left, right) => emitValDef(sym, "%s.join(%s)".format(quote(left), quote(right)))
      case red @ DListReduce(dlist, f) => emitValDef(sym, "%s.map(x => (x._1,x._2.reduce(%s)))".format(quote(dlist), handleClosure(red.closure)))
      case red @ DListReduceByKey(dlist, f) => emitValDef(sym, "%s.reduceByKey(%s)".format(quote(dlist), handleClosure(red.closure)))
      case v @ DListCache(dlist) => emitValDef(sym, "%s.cache()".format(quote(dlist)))
      case v @ DListCollect(dlist) => emitValDef(sym, "%s.collect()".format(quote(dlist)))
      case v @ DListTakeSample(in, r, n, s) => emitValDef(sym, "%s.takeSample(%s, %s, %s)".format(quote(in), quote(r), quote(n), quote(s)))
      case GetArgs() => emitValDef(sym, "sparkInputArgs.drop(1); // First argument is for spark context")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val inlineClosures = false

  var reduceByKey = true

  val allOff = false
  if (allOff) {
    narrowExistingMaps = false
    insertNarrowingMaps = false
    reduceByKey = false
    mapMerge = false
  }

  def transformTree[B: Manifest](block: Block[B]) = {
    var y = block
    // merge groupByKey with reduce to reduceByKey
    if (reduceByKey) {
      val rbkt = new ReduceByKeyTransformation()
      y = rbkt.run(y)
    }
    // inserting narrower maps and narrow them
    y = insertNarrowersAndNarrow(y, new SparkNarrowerInsertionTransformation())
    // TODO lower to loops
    y

  }

  val collectionName = "RDD"

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {

    val x = fresh[A]
    var y = reifyBlock(f(x))

    typeHandler = new TypeHandler(y)

    y = transformTree(y)

    stream.println("/*****************************************\n" +
      "  Emitting Spark Code                  \n" +
      "*******************************************/")
    stream.println("""
package spark.examples;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo

object %s {
        def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "spark.examples.Registrator_%s")

    		val sc = new SparkContext(sparkInputArgs(0), "%s")
        """.format(className, className, className))

    withStream(stream) {
      emitBlock(y)
    }
    stream.println("}")
    stream.println("}")
    stream.println("// Types that are used in this program")
    val restTypes = types.filterKeys(x => !skipTypes.contains(x))
    stream.println(restTypes.values.toList.sorted.mkString("\n"))

    stream.println("""class Registrator_%s extends KryoRegistrator {
        def registerClasses(kryo: Kryo) {
        %s
    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}""".format(className, types.keys.toList.sorted.map("kryo.register(classOf[" + _ + "])").mkString("\n")))
    stream.println("/*****************************************\n" +
      "  End of Spark Code                  \n" +
      "*******************************************/")

    stream.flush

    //writeGraphToFile(y, "test.dot", true)

    Nil
  }

}

trait SparkGen extends DListBaseCodeGenPkg with SparkGenDList

