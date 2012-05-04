package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import scala.virtualization.lms.common.BooleanOps
import scala.collection.mutable

trait DListTransformations extends ScalaGenBase with AbstractScalaGenDList with Matchers {

  
  
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const }
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
    DListNode
  }
  import IR.{ TTP, TP, SubstTransformer }
  import IR.{ findDefinition }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda }
  import IR.{ Struct }
/*  
  class TransformationState(val ttps: List[TTP], val results: List[Exp[Any]]) {
    def printAll(s: String = null) = {
      if (s != null)
        println("###################### " + s + " ######################")
      println("Printing all ttps for the current state")
      ttps.map(x => (x, " with types: " + x.lhs.map(_.Type).mkString(","))).foreach(println)
    }
  }


  val stopMarkingText = "stop transformation: node marked"

  class MarkerTransformer(val transformation: Transformation, val transformer: Transformer) extends SubstTransformer {
    private var toDo = mutable.HashSet[Exp[_]]()
    override def apply[A](inExp: Exp[A]): Exp[A] = {
      if (transformation.appliesToNode(inExp, transformer)) {
        toDo += inExp
        throw new InterruptedException(stopMarkingText)
      }
      inExp
    }
    def getTodo = toDo.toSet
  }

  class Transformer(var currentState: TransformationState) {

    var transformations: List[Transformation] = Nil

    def doTransformation(transformation: Transformation, limit: Int = 1) {
      transformations = List(transformation)
      stepUntilStable(limit)
    }

    def stepUntilStable(limit: Int = 20) = {
      var i = 0
      do {
        i += 1
      } while (doOneTransformation && (i < limit || limit < 0))
      if (i == limit) {
        System.err.println("Transformations " + transformations + " did not converge in " + limit + " steps")
      }
    }

    def doOneTransformation = {
      //      currentState.printAll()
      var out = false
      transformations.find { transformation =>
        val marker = new MarkerTransformer(transformation, this)
        try {
          transformAll(marker)
        } catch {
          case x: InterruptedException if (x.getMessage == stopMarkingText) => // expected, one value was found
          case x => throw (x)
        }
        for (exp <- marker.getTodo.take(1)) {
          out = true
          val (ttps, substs) = transformation.applyToNode(exp, this)
          var allAdded = Set[TTP]()
          var ttpsToAdd = ttps
          while (!ttpsToAdd.isEmpty) {
            val head = ttpsToAdd.head
            if (!currentState.ttps.contains(head)) {
              allAdded += head
              head match {
                case TTPDef(x) => {
                  val syms = readingNodes(x)
                  val newttps = syms.filter(IR.findDefinition(_).isDefined).map(IR.findOrCreateDefinition(_)).filter(_.sym.id > 3).map(fatten(_))
                  println("Adding ttps " + newttps)
                  ttpsToAdd ++= newttps
                }
                case TTP(x, IR.SimpleFatIfThenElse(cond, thenList, elseList)) => {
                  val allSyms = (List(cond) ++ ((thenList ++ elseList).map { case IR.Block(x) => x })).flatMap { case Def(x) => Some(x) case _ => None }
                  val newttps = allSyms.filter(IR.findDefinition(_).isDefined).map(IR.findOrCreateDefinition(_)).map(fatten(_))
                  ttpsToAdd ++= newttps
                }
                case x => println("did not match " + x + " while looking for dependencies")
              }
            }
            ttpsToAdd = ttpsToAdd.tail
          }
          val isPull = false && transformation.toString.contains("Pull")
          if (!allAdded.isEmpty && !isPull) {
            System.out.println("Applying " + transformation + " to node " + exp)
            println(printDef(exp) + " created new definitions: " + (allAdded).map(printDef).mkString(", "))
            println
          }
          val newState = new TransformationState(currentState.ttps ++ allAdded, currentState.results)
          val subst = new SubstTransformer()
          val substsAdd = substs.filter { case (Sym(i1), Sym(i2)) if (i1 == i2) => false case _ => true }
          subst.subst ++= substsAdd
          if (!substsAdd.isEmpty) {
            currentState = transformAll(subst, newState)
          }
        }
        out
      }
      //      currentState.printAll()
      out
    }

    def transformAll(marker: SubstTransformer, state: TransformationState = currentState) = {
      val after = transformAllFully(state.ttps, state.results, marker) match {
        case (scope, results) => new TransformationState(scope, results)
        case _ => state
      }
      after
    }

    def readingNodes(inExp: Def[_]) = {
      val reading = IR.syms(inExp).distinct
      val out = reading.flatMap { x => IR.findDefinition(x) }.map(_.rhs)
      //		  println("Reading nodes of "+inExp+" are "+reading+", with Defs "+out)
      out
    }
    def getConsumers(inExp: Exp[_]) = {
      val inSym = inExp.asInstanceOf[Sym[_]]
      val out = currentState.ttps.flatMap {
        _.rhs match {
          case ThinDef(x) => Some(x)
          case _ => None
        }
      }.filter(IR.findDefinition(_).isDefined)
        .filter { x => IR.syms(x).contains(inSym) }.distinct
      out
    }

  }

  trait Transformation {
    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean

    def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])])

    override def toString =
      this.getClass.getSimpleName.replaceAll("Transformation", "")
        .split("(?=[A-Z])").mkString(" ").trim

  }

  trait StandardTransformation extends Transformation {

    def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      val out = doTransformation(inExp);
      // make TTP's from defs
      val outTp = IR.findOrCreateDefinition(out)
      val ttps = List(fatten(outTp))
      // return ttps and substitutions
      val substs = List((inExp, outTp.sym))

      (ttps, substs)
    }

    def doTransformation(inExp: Exp[_]): Def[_]

  }

  trait SimpleTransformation extends StandardTransformation {
    override def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = {
      doTransformationPure(inExp) != null
    }
    final def doTransformation(inExp: Exp[_]) = {
      doTransformationPure(inExp)
    }
    def doTransformationPure(inExp: Exp[_]): Def[_]
  }

  trait SingleConsumerTransformation extends Transformation {
    override final def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = {
      if (!appliesToNodeImpl(inExp, t))
        false
      else {
        val out = t.readingNodes(IR.findDefinition(inExp.asInstanceOf[Sym[_]]).get.rhs)
        //.flatMap(_).fold(true)()
        out.map(x => t.getConsumers(IR.findDefinition(x).get.sym).size == 1).fold(true)(_ && _)
      }
    }
    def appliesToNodeImpl(inExp: Exp[_], t: Transformer): Boolean
  }

  trait SimpleSingleConsumerTransformation extends SimpleTransformation with SingleConsumerTransformation {
    override def appliesToNodeImpl(inExp: Exp[_], t: Transformer): Boolean = {
      doTransformationPure(inExp) != null
    }
  }

  class MergeFlattenTransformation extends Transformation {
    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(DListFlatten(list)) => {
        list.find { case Def(DListFlatten(list2)) => true case _ => false }.isDefined
      }
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      inExp match {
        case Def(lower @ DListFlatten(list)) =>
          val flat2 = list.find { case Def(DListFlatten(list2)) => true case _ => false }.get
          flat2 match {
            case d @ Def(upper @ DListFlatten(list2)) =>
              val out = new DListFlatten(list.filterNot(_ == d) ++ list2)
              var newDefs = List(out)
              val ttps = newDefs.map(IR.findOrCreateDefinition(_)).map(fatten)
              return (ttps, List((inExp, IR.findOrCreateDefinition(out).sym), (d, IR.findOrCreateDefinition(out).sym)))
          }
      }
      throw new RuntimeException("Bug in merge flatten")
    }

  }

  class SinkFlattenTransformation extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ DListMap(Def(vf @ DListFlatten(list)), func)) => {
        val mappers = list.map { x =>
          val mapper = new DListMap(x, func)
          val newDef = IR.toAtom2(mapper)
          newDef
        }
        new DListFlatten(mappers)
      }
      case _ => null
    }

  }

  class MapMergeTransformation extends SimpleSingleConsumerTransformation {

    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(red @ DListMap(Def(gbk @ DListMap(v1, f2)), f1)) => {
        DListMap(v1, f2.andThen(f1))(gbk.mA, red.mB)
      }
      case _ => null
    }
  }

  class PullDependenciesTransformation extends StandardTransformation {

    var _doneNodes = Set[Any]()

    final def appliesToNode(inExp: Exp[_], t: Transformer) = {
      if (_doneNodes.contains(inExp))
        false
      else
        appliesToNodeImpl(inExp: Exp[_], t: Transformer)
    }

    def appliesToNodeImpl(inExp: Exp[_], t: Transformer) = {
      inExp match {
        case Def(s: IR.ClosureNode[_, _]) => true
        case s: IR.Sym[_] if IR.findDefinition(s).isDefined => true
        case _ => false
      }
    }
    def doTransformation(inExp: Exp[_]): Def[_] = inExp match {
      case Def(s: IR.ClosureNode[_, _]) => _doneNodes += inExp; s
      case s: IR.Sym[_] if IR.findDefinition(s).isDefined => _doneNodes += inExp; IR.findDefinition(s).get.rhs
      case _ => null
    }
  }

  // TODO does not seem easily possible
  //	trait ComposePredicate {
  //	  def composePredicates[A : Manifest](p1 : Exp[A] => Exp[Boolean], p2 : Exp[A] => Exp[Boolean]) : Exp[A] => Exp[Boolean]
  //	}
  //	
  //	trait ComposePredicateBooleanOpsExp extends BooleanOps {
  //	  def composePredicates[A : Manifest](p1 : Exp[A] => Exp[Boolean], p2 : Exp[A] => Exp[Boolean]) : Exp[A] => Exp[Boolean] 
  //	  = { a : Exp[A] => infix_&&(p1(a),p2(a))}
  //	}
  //	
  //	trait FilterMergeTransformationHack extends SimpleSingleConsumerTransformation with ComposePredicate {
  ////		this : {def composePredicates[A : Manifest](p1 : Exp[A] => Exp[Boolean], p2 : Exp[A] => Exp[Boolean]) : Exp[A] => Exp[Boolean]}
  ////		=>
  //	   def doTransformationPure(inExp : Exp[_]) = inExp match {
  //            case Def(vf1@DListFilter(Def(vf2@DListFilter(v1, f1)),f2)) => {
  //              DListFilter(v1, composePredicates(f1,f2)(vf2.mA))(vf2.mA)
  //            }
  //            case _ => null
  //	   }
  //	}
  //
  //	class FilterMergeTransformation extends ComposePredicateBooleanOpsExp with FilterMergeTransformationHack 

  class MapNarrowTransformationNew(target: IR.Lambda[_, _], fieldReads: List[FieldRead], typeHandler: TypeHandler) extends SimpleTransformation {
    def this(target: DListNode with ClosureNode[_, _], typeHandler: TypeHandler) = this(
      target.closure match {
        case Def(l @ IR.Lambda(_, _, _)) => l
      }, target.successorFieldReads.toList, typeHandler)

    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ IR.Lambda(f, x, IR.Block(y))) if vm == target => {
        val fields = fieldReads.map(_.path)

        case class Node(val path: String, val children: mutable.Map[String, Node] = mutable.HashMap()) {
          def resolve(pathToChild: String): Option[Node] = {
            val newS = pathToChild.drop(5)
            val arg = if (newS.size >= 1) newS.drop(1) else ""
            resolveInternal(arg)
          }
          private def resolveInternal(pathToChild: String): Option[Node] = {
            if (!pathToChild.isEmpty) {
              val parts = (pathToChild.split("\\.", 2).toList ++ List("")).take(2)
              if (children.contains(parts.head)) {
                children(parts.head).resolveInternal(parts.last)
              } else {
                None
              }
            } else {
              Some(this)
            }
          }
        }
        val out = new Node("input")

        for (x <- fields) {
          var curNode = out
          for (y <- x.split("\\.").drop(1)) {
            if (!curNode.children.contains(y)) {
              val newNode = Node(y)
              curNode.children(y) = newNode
            }
            curNode = curNode.children(y)
          }
        }

        def build(path: String, readFromSym: Exp[_]): Exp[_] = {
          import typeHandler.{ TypeInfo, FieldInfo }
          val node = out.resolve(path).get
          val typeInfo = typeHandler.getTypeAt(path, vm.mB)
          typeInfo match {
            case ti @ TypeInfo(name, fields) => {
              val elems = for ((childName, node) <- node.children)
                yield (childName, build(path + "." + childName, readFromSym));
              IR.toAtom2(IR.SimpleStruct(name :: Nil, elems.toMap)(ti.m))(ti.m, FakeSourceContext())
            }
            case fi @ FieldInfo(name, niceType, position) => {
              val newSym = IR.field(readFromSym, name)(fi.m)
              //              val newSym = IR.toAtom2(IR.Field(readFromSym, name, fi.m))(fi.m, FakeSourceContext())
              if (node.children.isEmpty) {
                newSym
              } else {
                val elems = for ((childName, node) <- node.children)
                  yield (childName, build(path + "." + childName, newSym));
                val typ = fi.getType
                IR.toAtom2(IR.SimpleStruct(typ.name :: Nil, elems.toMap)(typ.m))(typ.m, FakeSourceContext())
              }

            }
          }
        }
        val newResult = build("input", y)

        IR.Lambda(f, x, IR.Block(newResult))(vm.mA, vm.mB)
      }
      case _ => null
    }
  }

  class InsertMapNarrowTransformation(target: DListNode, fields: List[FieldRead]) extends SimpleTransformation {
    var lastOut: Option[DListMap[_, _]] = None
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(x: DListNode) if x.metaInfos.contains("narrowed") => null
      case Def(x: ComputationNode) if target == x => {
        val typ = (x.getTypes._2.typeArguments(0))
        val out = DListMap(inExp.asInstanceOf[Exp[DList[Any]]], { x: IR.Rep[_] => x })(IR.mtype(typ), IR.mtype(typ))
        lastOut = Some(out)
        out.metaInfos("narrower") = true
        out.metaInfos("narrowed") = true
        out
      }
      case _ => null
    }

  }

  class FieldOnStructReadTransformation extends Transformation {

    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => true
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = inExp match {
      case d @ Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => {
        val outDef = elems.get(name).get
        (Nil, List((d, outDef)))
      }
      case _ => throw new RuntimeException("should not be called if appliesToNode returns false")
    }
  }

  class TypeTransformations(val typeHandler: TypeHandler) extends SimpleTransformation {
    lazy val set = typeHandler.typeInfos.keySet
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(t @ IR.SimpleStruct(x, y)) if set.contains(x.mkString("_")) => {
        val name = x.mkString("_")
        new IR.ObjectCreation(name, y)(t.m)
      }
      case _ => null
    }
  }
*/

}
