package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil

trait SparkProgram extends VectorOpsExp with VectorImplOps with SparkVectorOpsExp {

}

trait SparkVectorOpsExp extends VectorOpsExp {
  case class VectorReduceByKey[K: Manifest, V: Manifest](in: Exp[Vector[(K, V)]], func: (Exp[V], Exp[V]) => Exp[V])
      extends Def[Vector[(K, V)]]
      with PreservingTypeComputation[Vector[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    lazy val closure = doLambda2(func)(getClosureTypes._2, getClosureTypes._2, getClosureTypes._2)
    def getClosureTypes = (manifest[(V, V)], manifest[V])

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

trait SparkGenVector extends ScalaGenBase with ScalaGenVector with VectorTransformations with SparkTransformations {

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
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2 }
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

  object SomeDef {
    def unapply(x: Any): Option[Def[_]] = x match {
      case TTPDef(x) => Some(x)
      case x: Def[_] => Some(x)
      case Def(x) => Some(x)
      //	          case x => Some(x)
      case x => None //{ println("did not match " + x); None }
    }
  }

  object TTPDef {
    def unapply(ttp: TTP) = ttp match {
      case TTP(_, ThinDef(x)) => Some(x)
      case _ => None
    }
  }

  object FieldAccess {
    def unapply(ttp: TTP) = ttp match {
      case TTPDef(f @ Field(obj, field, typ)) => Some(f)
      case _ => None
    }
  }

  object ClosureNode {
    def unapply(any: Any) = any match {
      case TTPDef(cn: ClosureNode[_, _]) => Some(cn)
      case cn: ClosureNode[_, _] => Some(cn)
      case _ => None
    }
  }

  object SomeAccess {
    def unapply(ttp: Any) = ttp match {
      case SomeDef(IR.Tuple2Access1(d)) => Some((d, "._1"))
      case SomeDef(IR.Tuple2Access2(d)) => Some((d, "._2"))
      case SomeDef(IR.Field(struct, name, _)) => Some((struct, "." + name))
      case _ => None
    }
  }

  class Analyzer(state: TransformationState) {
    val nodes = state.ttps.flatMap {
      _ match {
        case TTPDef(x: VectorNode) => Some(x)
        case TTPDef(Reflect(x: VectorNode, _, _)) => Some(x)
        case _ => Nil
      }
    }
    val lambdas = state.ttps.flatMap {
      _ match {
        case TTPDef(l @ Lambda(f, x, y)) => Some(l)
        case _ => None
      }
    }
    val lambda2s = state.ttps.flatMap {
      _ match {
        case TTPDef(l @ IR.Lambda2(f, x1, x2, y)) => Some(l)
        case _ => None
      }
    }
    val saves = nodes.filter { case v: VectorSave[_] => true; case _ => false }

    def getInputs(x: VectorNode) = {
      val syms = IR.syms(x)
      syms.flatMap { x: Sym[_] => IR.findDefinition(x) }.flatMap { _.rhs match { case x: VectorNode => Some(x) case _ => None } }
    }

    val ordered = GraphUtil.stronglyConnectedComponents(saves, getInputs).flatten

    def getNodesForSymbol(x: Sym[_]) = {
      def getInputs(x: Sym[_]) = {
        IR.findDefinition(x) match {
          case Some(x) => IR.syms(x.rhs)
          case _ => Nil
        }
      }

      GraphUtil.stronglyConnectedComponents(List(x), getInputs).flatten.reverse
    }

    def getNodesInLambda(x: Any) = {
      x match {
        case Def(Lambda(_, _, IR.Block(y: Sym[_]))) => getNodesForSymbol(y)
        case Def(Lambda2(_, _, _, IR.Block(y: Sym[_]))) => getNodesForSymbol(y)
        case _ => Nil
      }
    }

    def getNodesInClosure(x: VectorNode) = x match {
      case x: ClosureNode[_, _] => getNodesInLambda(x.closure)
      case x: VectorReduce[_, _] => getNodesForSymbol(x.closure.asInstanceOf[Sym[_]])
      case x: VectorReduceByKey[_, _] => getNodesForSymbol(x.closure.asInstanceOf[Sym[_]])
      case _ => Nil
    }

    def pathToInput(node: Any, input: Sym[_], prefix: String = ""): Option[FieldRead] = {
      node match {
        case SomeAccess(nextNode, pathSegment) => pathToInput(nextNode, input, pathSegment + prefix)
        case x: Sym[_] if input == x => return Some(FieldRead("input" + prefix))
        case _ => return None
      }
    }

    def analyzeFunction(v: VectorNode) = {
      val nodes = getNodesInClosure(v).flatMap(IR.findDefinition(_)).map(_.rhs)
      val fields = nodes.filter { SomeAccess.unapply(_).isDefined }
      fields.flatMap(n => pathToInput(n, getNodesInClosure(v).head)).toSet
    }

    def computeFieldReads(node: VectorNode): Set[FieldRead] = {
      node match {
        case v @ VectorFilter(in, func) => analyzeFunction(v) ++ node.successorFieldReads
        case v @ VectorMap(in, func) if {
          val s1 = v.getTypes._2.toString
          s1.contains("Tuple") || s1.contains("Struct")
        } => {
          // backup TTPs, or create new transformer?
          val transformer = new Transformer(state)
          // create narrowing transformation for this map
          val narrowMapTransformation = new MapNarrowTransformation(v, node.successorFieldReads.toList)
          transformer.transformations = List(narrowMapTransformation)
          // run transformer
          if (!transformer.doOneTransformation) {
            println("Transformation failed for " + node)
          }
          // analyze field reads of the new function
          val a2 = new Analyzer(transformer.currentState)
          a2.analyzeFunction(narrowMapTransformation.lastOut)
        }
        case v @ VectorMap(in, func) => analyzeFunction(v)
        case _ => Set()
      }
    }

    def makeFieldAnalysis {
      nodes.foreach {
        node =>
          node.directFieldReads.clear
          node.successorFieldReads.clear
      }

      ordered.foreach {
        node =>
          val reads = computeFieldReads(node)
          node.directFieldReads ++= reads
          getInputs(node).foreach { _.successorFieldReads ++= reads }
          println("Computed field reads for " + node + " got " + reads)
      }
    }

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

