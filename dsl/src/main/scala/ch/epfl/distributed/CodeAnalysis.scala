package ch.epfl.distributed

import scala.virtualization.lms.util.GraphUtil
import scala.collection.mutable.Buffer
import java.util.regex.Pattern

trait VectorAnalysis extends ScalaGenVector with VectorTransformations with Matchers {

  val IR: VectorOpsExp
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
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  class Analyzer(state: TransformationState) {
    val nodes = state.ttps.flatMap {
      _ match {
        case TTPDef(x: VectorNode) => Some(x)
        case TTPDef(Reflect(x: VectorNode, _, _)) => Some(x)
        case _ => None
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
      case x: Closure2Node[_, _, _] => getNodesInLambda(x.closure)
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
        case v @ VectorMap(in, func) if !v.getClosureTypes._2.erasure.isPrimitive => {
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
          val candidates = transformer.currentState.ttps.flatMap {
            case TTPDef(vm @ VectorMap(in2, _)) if (in2 == in) => Some(vm)
            case _ => None
          }
          println("candidates for map to be analyzed " + candidates)
          a2.analyzeFunction(candidates.head)
        }
        case v @ VectorMap(in, func) => analyzeFunction(v)
        case v @ VectorReduce(in, func) =>
          analyzeFunction(v)
            .map(_.path.drop(5))
            .map(x => "input._2.iterable" + x)
            .map(FieldRead)
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

    def getIdForNode(n: VectorNode with Def[_]) = {
      //      nodes.indexOf(n)
      val op1 = IR.findDefinition(n.asInstanceOf[Def[_]])
      if (op1.isDefined) {
        op1.get.sym.id
      } else {
        state.ttps.flatMap {
          case TTP(Sym(s) :: _, ThinDef(Reflect(x, _, _))) if x == n => Some(s)
          //          case TTPDef(Reflect(s@Def(x),_,_)) if x==n => Some(s)
          case _ => None
        }.head
      }
    }

    def exportToGraph = {
      val buf = Buffer[String]()
      buf += "digraph g {"
      for (node <- nodes) {
        buf += """%s [label="%s(%s)"];""".format(getIdForNode(node), node.toString.takeWhile(_ != '('), getIdForNode(node))
      }
      for (node <- nodes; input <- getInputs(node)) {
        buf += """%s -> %s [label="%s"]; """.format(getIdForNode(input), getIdForNode(node),
          node.directFieldReads.map(_.path).toList.sortBy(x => x).mkString(","))
      }
      buf += "}"
      buf.mkString("\n")
    }

  }
}