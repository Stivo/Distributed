package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import scala.virtualization.lms.common.BooleanOps
import scala.collection.mutable

trait VectorTransformations extends ScalaGenBase with ScalaGenVector {

  val IR: VectorOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const }
  import IR.{
    NewVector,
    VectorSave,
    VectorMap,
    VectorFilter,
    VectorFlatMap,
    VectorFlatten,
    VectorGroupByKey,
    VectorReduce,
    ComputationNode
  }
  import IR.{ TTP, TP, SubstTransformer, ThinDef }
  import IR.{ findDefinition }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda }
  import IR.{ Struct }

  class MarkerTransformer(val transformation: Transformation, val transformer: Transformer) extends SubstTransformer {
    private var toDo = mutable.HashSet[Exp[_]]()
    override def apply[A](inExp: Exp[A]): Exp[A] = {
      if (transformation.appliesToNode(inExp, transformer)) {
        toDo += inExp
      }
      inExp
    }
    def getTodo = toDo.toSet
  }

  class Transformer(var currentState: TransformationState, var transformations: List[Transformation] = Nil) {

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
      currentState.printAll
      var out = false
      transformations.find { transformation =>
        val marker = new MarkerTransformer(transformation, this)
        transformAll(marker)
        for (exp <- marker.getTodo.take(1)) {
          out = true
          System.out.println("Applying " + transformation + " to node " + exp)
          val (ttps, substs) = transformation.applyToNode(exp, this)
          val newState = new TransformationState(currentState.ttps ++ ttps, currentState.results)
          val subst = new SubstTransformer()
          subst.subst ++= substs
          currentState = transformAll(subst, newState)
        }
        out
      }
      currentState.printAll
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
      val reading = IR.syms(inExp).removeDuplicates
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
        .filter { x => IR.syms(x).contains(inSym) }.removeDuplicates
      out
    }

  }

  class TransformationState(val ttps: List[TTP], val results: List[Exp[Any]]) {
    def printAll = {
      //	    println("Printing all ttps for the current state")
      //	    ttps.foreach(println)
    }
  }

  trait Transformation {

    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean

    def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      val out = doTransformation(inExp);
      // get dependencies
      val readers = transformer.readingNodes(out)
      // find all new Defs
      var newDef = out
      // make TTP's from defs
      val ttps = List(newDef).map(IR.findOrCreateDefinition(_)).map(fatten)
      // return ttps and substitutions
      (ttps, List((inExp, IR.findOrCreateDefinition(out).sym)))
    }
    def doTransformation(inExp: Exp[_]): Def[_]

    override def toString =
      this.getClass.getSimpleName.replaceAll("Transformation", "")
        .split("(?=[A-Z])").mkString(" ")

  }

  class SinkFlattenTransformation extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ VectorMap(Def(vf @ VectorFlatten(list)), func)) => {
        val mappers = list.map { x =>
          val mapper = new VectorMap(x, func)
          val newDef = IR.toAtom2(mapper)
          newDef
        }
        new VectorFlatten(mappers)
      }
      case _ => null
      // match error is ok, should not happen
    }

  }

  class MergeFlattenTransformation extends Transformation {
    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(VectorFlatten(list)) => {
        list.find { case Def(VectorFlatten(list2)) => true case _ => false }.isDefined
      }
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      inExp match {
        case Def(lower @ VectorFlatten(list)) =>
          val flat2 = list.find { case Def(VectorFlatten(list2)) => true case _ => false }.get
          flat2 match {
            case d @ Def(upper @ VectorFlatten(list2)) =>
              val out = new VectorFlatten(list.filterNot(_ == d) ++ list2)
              var newDefs = List(out)
              val ttps = newDefs.map(IR.findOrCreateDefinition(_)).map(fatten)
              return (ttps, List((inExp, IR.findOrCreateDefinition(out).sym), (d, IR.findOrCreateDefinition(out).sym)))
          }
      }
      throw new RuntimeException("Bug in merge flatten")
    }

    final def doTransformation(inExp: Exp[_]) = {
      throw new RuntimeException("Should not be called directly")
    }

  }

  trait SimpleTransformation extends Transformation {
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

  class MapMergeTransformation extends SimpleSingleConsumerTransformation {

    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(red @ VectorMap(Def(gbk @ VectorMap(v1, f2)), f1)) => {
        VectorMap(v1, f2.andThen(f1))(gbk.mA, red.mB)
      }
      case _ => null
    }
  }

  class PullDependenciesTransformation extends Transformation {

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
  //            case Def(vf1@VectorFilter(Def(vf2@VectorFilter(v1, f1)),f2)) => {
  //              VectorFilter(v1, composePredicates(f1,f2)(vf2.mA))(vf2.mA)
  //            }
  //            case _ => null
  //	   }
  //	}
  //
  //	class FilterMergeTransformation extends ComposePredicateBooleanOpsExp with FilterMergeTransformationHack 

  // TODO WIP
  def narrow[A: Manifest](fields: List[String]) = { in: IR.Rep[_] =>
    in match {
      case Def(IR.SimpleStruct(tag, elems)) => IR.toAtom2(new IR.SimpleStruct[A](tag, elems.filterKeys(fields.contains)))
      case _ => in
    }
  }

  class MapNarrowTransformation extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ VectorMap(in, f)) => {
        val narrower = narrow(List("im"))(vm.mB)
        //	    	 val lifted = IR.toAtom2(narrower)
        VectorMap(in, f.andThen(narrower))(vm.mA, vm.mB)
      }
      case _ => null
    }
  }

}
