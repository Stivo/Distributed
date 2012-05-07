/*package ch.epfl.distributed

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
    def cache = dlist_cache(dlist)
  }

  def dlist_cache[A: Manifest](dlist: Rep[DList[A]]): Rep[DList[A]]
}

trait SparkDListOpsExp extends DListOpsExp with SparkDListOps {
  case class DListReduceByKey[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]], func: (Exp[V], Exp[V]) => Exp[V])
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

  override def dlist_cache[A: Manifest](in: Rep[DList[A]]) = DListCache[A](in)

  //  override def dlist_map[A: Manifest, B: Manifest](dlist: Exp[DList[A]], f: Exp[A] => Exp[B]) = dlist match {
  //    case Def(vm @ DListMap(in, f2)) => DListMap(dlist, f2.andThen(f))(mtype(vm.mA), manifest[B]) 
  //    case _ => super.dlist_map(dlist, f)
  //  }

  override def syms(e: Any): List[Sym[Any]] = e match {
    // DListReduceBykey is a closure2node, handled by superclass
    case v: DListCache[_] => syms(v.in)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    // DListReduceBykey is a closure2node, handled by superclass
    case v: DListCache[_] => freqNormal(v.in)
    case _ => super.symsFreq(e)
  }

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = {
    val out = (e match {
      case v @ DListReduceByKey(dlist, func) => toAtom(
        new { override val overrideClosure = Some(f(v.closure)) } with DListReduceByKey(f(dlist), f(func))(v.mKey, v.mValue)
      )(mtype(manifest[A]))
      case v @ DListCache(in) => toAtom(DListCache(f(in))(v.mA))(mtype(v.getType))
      case _ => super.mirror(e, f)
    })
    copyMetaInfo(e, out)
    out.asInstanceOf[Exp[A]]
  }
}

trait SparkTransformations extends DListTransformations {
  val IR: DListOpsExp with SparkDListOpsExp
  import IR.{ DListReduceByKey, DListReduce, DListGroupByKey, DListMap }
  import IR.{ Def, Exp }

  class ReduceByKeyTransformation extends SimpleSingleConsumerTransformation {
    // TODO: apply if all consumers of group by key are reduces,
    // even if there is more than one
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(red @ DListReduce(Def(gbk @ DListGroupByKey(v1)), f1)) => {
        new DListReduceByKey(v1, f1)(red.mKey, red.mValue)
      }
      case _ => null
    }
  }

  class PullSparkDependenciesTransformation extends PullDependenciesTransformation {

    override def appliesToNodeImpl(inExp: Exp[_], t: Transformer) = {
      inExp match {
        case Def(DListReduceByKey(in, func)) => true
        case _ => super.appliesToNodeImpl(inExp, t)
      }
    }
    override def doTransformation(inExp: Exp[_]): Def[_] = inExp match {
      case Def(r @ DListReduceByKey(in, func)) => _doneNodes += inExp; r
      case _ => super.doTransformation(inExp)
    }
  }

}

trait SparkDListAnalysis extends DListAnalysis {
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
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def newAnalyzer(state: TransformationState, typeHandlerForUse: TypeHandler = typeHandler) = new SparkAnalyzer(state, typeHandlerForUse)

  class SparkAnalyzer(state: TransformationState, typeHandler: TypeHandler) extends Analyzer(state, typeHandler) {

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

      case v @ DListCache(in) => node.successorFieldReads.toSet

      case _ => super.computeFieldReads(node)
    }
  }

}

trait SparkGenDList extends ScalaGenBase with ScalaGenDList with DListTransformations
    with SparkTransformations with Matchers with SparkDListAnalysis with CaseClassTypeFactory {

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
    ComputationNode,
    DListNode,
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
        var out = "(" + v1.map(quote(_)).mkString(").union(")
        out += ")"
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist) => emitValDef(sym, "%s.groupByKey".format(quote(dlist)))
      case v @ DListJoin(left, right) => emitValDef(sym, "%s.join(%s)".format(quote(left), quote(right)))
      case red @ DListReduce(dlist, f) => emitValDef(sym, "%s.map(x => (x._1,x._2.reduce(%s)))".format(quote(dlist), handleClosure(red.closure)))
      case red @ DListReduceByKey(dlist, f) => emitValDef(sym, "%s.reduceByKey(%s)".format(quote(dlist), handleClosure(red.closure)))
      case v @ DListCache(dlist) => emitValDef(sym, "%s.cache()".format(quote(dlist)))
      case GetArgs() => emitValDef(sym, "sparkInputArgs.drop(1); // First argument is for spark context")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val inlineClosures = true

  var reduceByKey = true

  val allOff = false
  if (allOff) {
    narrowExistingMaps = false
    insertNarrowingMaps = false
    reduceByKey = false
    mapMerge = false

  }

  override def newPullDeps = new PullSparkDependenciesTransformation

  override def transformTree(state: TransformationState): TransformationState = {
    val transformer = new Transformer(state)
    var pullDeps = newPullDeps
    transformer.doTransformation(pullDeps, 500)

    // perform spark optimizations
    if (mapMerge) {
      //      transformer.currentState.printAll("Before map merge")
      transformer.doTransformation(new MapMergeTransformation, 500)
      transformer.doTransformation(pullDeps, 500)
      //      transformer.currentState.printAll("After map merge")
    }
    if (reduceByKey) {
      transformer.doTransformation(new ReduceByKeyTransformation, 500)
      //pullDeps = new PullSparkDependenciesTransformation()
      transformer.doTransformation(pullDeps, 500)
    }
    mapNarrowingAndInsert(transformer)
    transformer.doTransformation(pullDeps, 500)
    transformer.doTransformation(new TypeTransformations(typeHandler), 500)
    transformer.doTransformation(pullDeps, 500)

    writeGraphToFile(transformer, "test.dot", true)

    transformer.currentState
  }

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    //    val func : Exp[A] => Exp[B] = {x => reifyEffects(f(x))}

    val x = fresh[A]
    val y = reifyBlock(f(x))

    val sA = mA.toString
    val sB = mB.toString

    //    val staticData = getFreeDataBlock(y)

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

    emitBlock(y)

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

    //    staticData
    Nil
  }

}

trait SparkGen extends DListBaseCodeGenPkg with SparkGenDList

*/ 