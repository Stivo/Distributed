package ch.epfl.distributed

import scala.virtualization.lms.util.GraphUtil
import scala.collection.mutable.Buffer
import scala.collection.mutable
import java.util.regex.Pattern
import scala.util.Random

trait VectorAnalysis extends AbstractScalaGenVector with VectorTransformations with Matchers {

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
    VectorJoin,
    VectorReduce,
    ComputationNode,
    VectorNode,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, ThinDef, Field }
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  def newAnalyzer(state: TransformationState, typeHandlerForUse: TypeHandler = typeHandler) = new Analyzer(state, typeHandlerForUse)

  class Analyzer(state: TransformationState, typeHandler: TypeHandler) {
    lazy val nodes = state.ttps.flatMap {
      _ match {
        case TTPDef(x: VectorNode) => Some(x)
        case TTPDef(Reflect(x: VectorNode, _, _)) => Some(x)
        case _ => None
      }
    }
    lazy val lambdas = state.ttps.flatMap {
      _ match {
        case TTPDef(l @ Lambda(f, x, y)) => Some(l)
        case _ => None
      }
    }
    lazy val lambda2s = state.ttps.flatMap {
      _ match {
        case TTPDef(l @ IR.Lambda2(f, x1, x2, y)) => Some(l)
        case _ => None
      }
    }
    lazy val saves = nodes.filter { case v: VectorSave[_] => true; case _ => false }

    def getInputs(x: VectorNode) = {
      val syms = IR.syms(x)
      syms.flatMap { x: Sym[_] => IR.findDefinition(x) }.flatMap { _.rhs match { case x: VectorNode => Some(x) case _ => None } }
    }

    lazy val ordered = GraphUtil.stronglyConnectedComponents(saves, getInputs).flatten

    lazy val narrowBeforeCandidates: Iterable[VectorNode] = nodes.filter(isNarrowBeforeCandidate)

    def isNarrowBeforeCandidate(x: VectorNode) = x match {
      case VectorGroupByKey(x) => true
      case VectorJoin(x, y) => true
      case x => false
    }

    lazy val narrowBefore: Iterable[VectorNode] = narrowBeforeCandidates
      .filter { x =>
        getInputs(x).size != x.metaInfos.getOrElse("insertedNarrowers", 0)
      }
      .filter {
        case x: ComputationNode => !isSimpleType(x.getElementTypes._1)
        case VectorJoin(l, r) => true
        case _ => throw new RuntimeException("Add narrow before candidate here or in subclass")
      }

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

    import typeHandler._
    def visitAll(path: String, m: Manifest[_]) = {
      val reads = new mutable.HashSet[String]()
      val typ = typeHandler.getTypeAt(path, m)

      def recurse(path: String, typ: PartInfo[_]) {
        reads += path
        typ match {
          case TypeInfo(name, fields) => fields.foreach(x => recurse(path + "." + x.name, x))
          case FieldInfo(name, typ, pos) if typeHandler.typeInfos2.contains(typ) =>
            typeHandler.typeInfos2(typ).fields.foreach(x => recurse(path + "." + x.name, x))
          case _ =>
        }
      }
      recurse(path, typ)
      reads.map(FieldRead).toSet
    }

    def computeFieldReads(node: VectorNode): Set[FieldRead] = {
      val out: Set[FieldRead] = node match {
        case NewVector(_) => Set()

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
          val narrowMapTransformation = new MapNarrowTransformationNew(v, typeHandler)
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
          val a2 = newAnalyzer(transformer.currentState, typeHandler)
          val candidates = transformer.currentState.ttps.flatMap {
            case TTPDef(vm @ VectorMap(_, _)) if (vm.metaInfos.contains(id)) => Some(vm)
            case _ => None
          }
          // remove the tag, not needed afterwards
          v.metaInfos.remove(id)
          a2.analyzeFunction(candidates.head)
        }

        case v @ VectorMap(in, func) => analyzeFunction(v)

        case v @ VectorJoin(Def(left: VectorNode), Def(right: VectorNode)) =>
          def fieldRead(x: List[String]) = FieldRead("input." + x.mkString("."))
          val reads = v.successorFieldReads.map(_.getPath.drop(1)).map {
            case "_2" :: "_1" :: x => List(left) -> fieldRead("_2" :: x)
            case "_2" :: "_2" :: x => List(right) -> fieldRead("_2" :: x)
            case "_1" :: x => List(left, right) -> fieldRead("_1" :: x)
            case x => Nil -> null
          }
          reads.foreach { case (targets, read) => targets.foreach { _.successorFieldReads += read } }
          visitAll("input._1", v.mIn1)

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
          }.map(_.mkString(".")))).map(FieldRead).toSet ++ visitAll("input._1", v.mInType)

        case v @ VectorFlatten(inputs) =>
          // just pass on the successor field reads
          v.successorFieldReads.toSet

        case v @ VectorSave(_, _) => {
          if (SimpleType.unapply(v.mA).isDefined) {
            Set()
          } else {
            // traverse all subtypes, mark all fields as read
            visitAll("input", v.mA)
          }
        }

        case v @ VectorFlatMap(in, func) => analyzeFunction(v)

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

    var addComments = true

    def exportToGraph = {
      val buf = Buffer[String]()
      buf += "digraph g {"
      for (node <- nodes) {
        var comment = ""
        if (addComments && !node.metaInfos.isEmpty) {
          comment += "\\n" + node.metaInfos
        }
        buf += """%s [label="%s(%s)%s"];"""
          .format(getIdForNode(node), node.toString.takeWhile(_ != '('), getIdForNode(node),
            comment)
      }
      for (node <- nodes; input1 <- getInputs(node)) {
        buf += """%s -> %s [label="%s"]; """.format(getIdForNode(input1), getIdForNode(node),
          input1.successorFieldReads.map(_.path).toList.sortBy(x => x).mkString(","))
      }
      buf += "}"
      buf.mkString("\n")
    }

  }
}
