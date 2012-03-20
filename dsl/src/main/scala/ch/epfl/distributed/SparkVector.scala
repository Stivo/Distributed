package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream

trait SparkProgram extends VectorOpsExp with VectorImplOps with SparkVectorOpsExp {

}

trait SparkVectorOpsExp extends VectorOpsExp {
  case class VectorReduceByKey[K: Manifest, V: Manifest](in: Exp[Vector[(K, V)]], func: (Exp[V], Exp[V]) => Exp[V])
      extends Def[Vector[(K, V)]] with Closure2Node[V, V, V]
      with PreservingTypeComputation[Vector[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getType = manifest[Vector[(K, V)]]
  }

  override def syms(e: Any): List[Sym[Any]] = e match {
    case red: VectorReduceByKey[_, _] => syms(red.in, red.closure)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case red: VectorReduceByKey[_, _] => freqHot(red.closure) ++ freqNormal(red.in)
    case _ => super.symsFreq(e)
  }

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = {
    (e match {
      case v @ VectorReduceByKey(vector, func) => toAtom(VectorReduceByKey(f(vector), f(func))(v.mKey, v.mValue))
      case _ => super.mirror(e, f)
    }).asInstanceOf[Exp[A]]
  }
}

trait SparkTransformations extends VectorTransformations {
  val IR: VectorOpsExp with SparkVectorOpsExp
  import IR.{ VectorReduceByKey, VectorReduce, VectorGroupByKey, VectorMap }
  import IR.{ Def, Exp }

  class ReduceByKeyTransformation extends SimpleSingleConsumerTransformation {
    // TODO: apply if all consumers of group by key are reduces,
    // even if there is more than one
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(red @ VectorReduce(Def(gbk @ VectorGroupByKey(v1)), f1)) => {
        new VectorReduceByKey(v1, f1)(red.mKey, red.mValue)
      }
      case _ => null
    }
  }

  class PullSparkDependenciesTransformation extends PullDependenciesTransformation {

    override def appliesToNodeImpl(inExp: Exp[_], t: Transformer) = {
      inExp match {
        case Def(VectorReduceByKey(in, func)) => true
        case _ => super.appliesToNodeImpl(inExp, t)
      }
    }
    override def doTransformation(inExp: Exp[_]): Def[_] = inExp match {
      case Def(r @ VectorReduceByKey(in, func)) => _doneNodes += inExp; r
      case _ => super.doTransformation(inExp)
    }
  }

}

trait SparkGenVector extends ScalaGenBase with ScalaGenVector with VectorTransformations with SparkTransformations with Matchers with VectorAnalysis {

  val IR: SparkVectorOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    NewVector,
    VectorSave,
    VectorMap,
    VectorFilter,
    VectorFlatMap,
    VectorFlatten,
    VectorGroupByKey,
    VectorReduce,
    ComputationNode,
    VectorNode,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, ThinDef, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ VectorReduceByKey }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) =
    {
      val out = rhs match {
        case IR.SimpleStruct(tag, elems) => emitValDef(sym, "Creating struct with %s and elems %s".format(tag, elems))
        case nv @ NewVector(filename) => emitValDef(sym, "sc.textFile(%s)".format(quote(filename)))
        case vs @ VectorSave(vector, filename) => stream.println("%s.saveAsTextFile(%s)".format(quote(vector), quote(filename)))
        case vm @ VectorMap(vector, function) => emitValDef(sym, "%s.map(%s)".format(quote(vector), quote(vm.closure)))
        case vf @ VectorFilter(vector, function) => emitValDef(sym, "%s.filter(%s)".format(quote(vector), quote(vf.closure)))
        case vm @ VectorFlatMap(vector, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(vector), quote(vm.closure)))
        case vm @ VectorFlatten(v1) => {
          var out = "(" + v1.map(quote(_)).mkString(").union(")
          out += ")"
          emitValDef(sym, out)
        }
        case gbk @ VectorGroupByKey(vector) => emitValDef(sym, "%s.groupByKey".format(quote(vector)))
        case red @ VectorReduce(vector, f) => emitValDef(sym, "%s.map(x => (x._1,x._2.reduce(%s)))".format(quote(vector), quote(red.closure)))
        case red @ VectorReduceByKey(vector, f) => emitValDef(sym, "%s.reduceByKey(%s)".format(quote(vector), quote(red.closure)))
        case GetArgs() => emitValDef(sym, "sparkInputArgs.drop(1); // First argument is for spark context")
        case _ => super.emitNode(sym, rhs)
      }
      //    println(sym+" "+rhs)
      out
    }

  override def focusExactScopeFat[A](currentScope0In: List[TTP])(result0B: List[Block[Any]])(body: List[TTP] => A): A = {
    val hasVectorNodes = !currentScope0In.flatMap { TTPDef.unapply }.flatMap { case x: VectorNode => Some(x) case _ => None }.isEmpty
    if (hasVectorNodes) {
      // set up transformer
      var result0 = result0B.map(getBlockResultFull)
      var state = new TransformationState(currentScope0In, result0)
      val transformer = new Transformer(state)
      val pullDeps = new PullSparkDependenciesTransformation()

      // perform spark optimizations
      // transformer.doTransformation(new MapMergeTransformation, 500)
      //      transformer.doTransformation(new ReduceByKeyTransformation, 500)
      transformer.doTransformation(pullDeps, 500)

      // perform field usage analysis
      val analyzer = new Analyzer(transformer.currentState)
      println("################# here #####################")
      println(analyzer.ordered)
      println(analyzer.lambdas)
      println(analyzer.lambda2s)
      analyzer.ordered.foreach(x => println(x + " " + analyzer.getNodesInClosure(x)))
      analyzer.makeFieldAnalysis
      val out = new FileOutputStream("test.dot")
      out.write(analyzer.exportToGraph.getBytes)
      out.close
      println("############# HERE END #####################")
      //      transformer.currentState.ttps.foreach(println)
      // insert vectormaps and replace creation of structs with narrower types

      state = transformer.currentState
      val currentScope0 = state.ttps
      result0 = state.results

      super.focusExactScopeFat(currentScope0)(result0.map(IR.Block(_)))(body)
    } else {
      super.focusExactScopeFat(currentScope0In)(result0B)(body)
    }
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

object %s {
        def main(sparkInputArgs: Array[String]) {
    		val sc = new SparkContext(sparkInputArgs(0), "%s")
        """.format(className, className))

    // TODO: separate concerns, should not hard code "pxX" name scheme for static data here

    emitBlock(y)(stream)

    stream.println("}")

    stream.println("}")
    stream.println("/*****************************************\n" +
      "  End of Spark Code                  \n" +
      "*******************************************/")

    stream.flush

    //    staticData
    Nil
  }

}

trait SparkGen extends VectorBaseCodeGenPkg with SparkGenVector

