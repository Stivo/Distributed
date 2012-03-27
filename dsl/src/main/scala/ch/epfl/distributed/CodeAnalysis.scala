package ch.epfl.distributed

import scala.virtualization.lms.util.GraphUtil
import scala.collection.mutable.Buffer
import scala.collection.mutable
import java.util.regex.Pattern
import scala.util.Random

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

  class Analyzer(state: TransformationState, typeHandler: TypeHandler) {
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
      val out: Set[FieldRead] = node match {
        case v @ VectorFilter(in, func) => analyzeFunction(v) ++ node.successorFieldReads

        case v @ VectorMap(in, func) if !v.metaInfos.contains("narrowed")
          && !SimpleType.unapply(v.getClosureTypes._2).isDefined
          && hasObjectCreationInClosure(v) => {
          // backup TTPs, or create new transformer?
          val transformer = new Transformer(state)
          // tag this map to recognize it after transformations
          val id = "id_" + Random.nextString(20)
          v.metaInfos(id) = true
          // create narrowing transformation for this map
          var ll: IR.Lambda[_, _] = v.closure match {
            case Def(l @ IR.Lambda(_, _, _)) => l
          }
          val narrowMapTransformation = new MapNarrowTransformationNew(ll, node.successorFieldReads.toList, typeHandler)
          transformer.transformations = List(narrowMapTransformation)
          // run transformation
          if (!transformer.doOneTransformation) {
            println("Transformation failed for " + node + " during field analysis")
          }
          val pullDeps = new PullDependenciesTransformation()
          transformer.doTransformation(pullDeps, 500)
          transformer.doTransformation(new FieldOnStructReadTransformation, 500)
          transformer.doTransformation(pullDeps, 500)
          // analyze field reads of the new function
          val a2 = new Analyzer(transformer.currentState, typeHandler)
          val candidates = transformer.currentState.ttps.flatMap {
            case TTPDef(vm @ VectorMap(_, _)) if (vm.metaInfos.contains(id)) => Some(vm)
            case _ => None
          }
          // remove the tag, not needed afterwards
          v.metaInfos.remove(id)
          a2.analyzeFunction(candidates.head)
        }

        case v @ VectorMap(in, func) => analyzeFunction(v)

        case v @ VectorReduce(in, func) =>
          // analyze function
          // convert the analyzed accesses to accesses of input._2.iterable
          val part1 = (analyzeFunction(v) ++ Set(FieldRead("input")))
            .map(_.path.drop(5))
            .map(x => "input._2.iterable" + x)
          // add the iterable to the path for reads from successors
          val part2 = v.successorFieldReads.map { _.getPath }.map {
            case "input" :: "_2" :: x => "input" :: "_2" :: "iterable" :: x
            case x => x
          }.map(_.mkString("."))
          (part1 ++ part2).map(FieldRead)

        case v @ VectorGroupByKey(in) =>
          // rewrite access to input._2.iterable.X to input._2.X
          // add access to _1
          ((v.successorFieldReads.map(_.getPath).map {
            case "input" :: "_2" :: "iterable" :: x => "input" :: "_2" :: x
            case x => x
          }.map(_.mkString("."))) ++ Set("input._1")).map(FieldRead).toSet

        case v @ VectorFlatten(inputs) =>
          // just pass on the successor field reads
          v.successorFieldReads.toSet

        case v @ VectorSave(_, _) => {
          if (SimpleType.unapply(v.mA).isDefined) {
            Set()
          } else {
            import typeHandler._
            // traverse all subtypes, mark all fields as read
            val typ = typeHandler.getTypeAt("input", v.mA)
            val reads = new mutable.HashSet[String]()
            def visitAll(path: String, typ: PartInfo[_]) {
              reads += path
              typ match {
                case TypeInfo(name, fields) => fields.foreach(x => visitAll(path + "." + x.name, x))
                case FieldInfo(name, typ, pos) if typeHandler.typeInfos2.contains(typ) =>
                  typeHandler.typeInfos2(typ).fields.foreach(x => visitAll(path + "." + x.name, x))
                case _ =>
              }
            }
            visitAll("input", typ)
            reads.map(FieldRead).toSet
          }
        }

        case NewVector(_) => Set()

        case x => throw new RuntimeException("Need to implement field analysis for " + x)
        //Set[FieldRead]()
      }
      out
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

    def hasObjectCreationInClosure(v: VectorNode) = {
      val nodes = getNodesInClosure(v)
      nodes.find { case Def(s: IR.SimpleStruct[_]) => true case _ => false }.isDefined
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