package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import scala.virtualization.lms.common.BooleanOps
import scala.collection.mutable

trait VectorTransformations extends ScalaGenBase with ScalaGenVector with Matchers {

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
      currentState.printAll
      var out = false
      transformations.find { transformation =>
        val isPull = transformation.toString.contains("Pull")
        val marker = new MarkerTransformer(transformation, this)
        transformAll(marker)
        for (exp <- marker.getTodo.take(1)) {
          out = true
          val (ttps, substs) = transformation.applyToNode(exp, this)
          if (!isPull) {
            System.out.println("Applying " + transformation + " to node " + exp)
            println(printDef(exp) + " => " + printDef(ttps.head))
            println
          }
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
      val substs = List((inExp, IR.findOrCreateDefinition(out).sym))
      (ttps, substs)
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

  // TODO Nesting
  def narrow[A: Manifest](fields: List[FieldRead]) = { in: IR.Rep[_] =>
    println("narrow called with " + fields)
    def trim(in: IR.Rep[_], path: String = "input"): IR.Rep[_] = {
      println("trim called with " + path + " " + in)
      println(in match {
        case Def(IR.SimpleStruct(tag, elems)) => "is a simple struct with " + tag + " " + elems
        case Def(x) => "is some def " + x
        case _ => "is not a simple struct"
      })
      in match {
        case Def(IR.SimpleStruct(tag, elems)) => IR.toAtom2(new IR.SimpleStruct[A](tag,
          elems.filterKeys(key => fields.map(_.path).find(_.startsWith(path + "." + key)).isDefined)
            .map { case (k, v) => (k, trim(v, path + "." + k)) }))
        case _ => in
      }
    }
    /*
    in match {
      case Def(IR.SimpleStruct(tag, elems)) => {new IR.SimpleStruct[A](tag, elems.filterKeys(topLevel.contains))}
      case Def(inDef) => {println("#%%%" +inDef); in}
      case _ => {"#%%" +println(in); in}
    }
    */
    trim(in)
  }

  class MapNarrowTransformation(target: IR.VectorNode, fields: List[FieldRead]) extends SimpleTransformation {
    var lastOut: VectorMap[_, _] = null
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ VectorMap(in, f)) if vm == target => {
        //      case Def(vm @ VectorMap(in, f)) => {
        val narrower = narrow(fields)(vm.mB)
        //	    	 val lifted = IR.toAtom2(narrower)
        val out = VectorMap(in, f.andThen(narrower))(vm.mA, vm.mB)
        lastOut = out
        out
      }
      case _ => null
    }
  }

  class TupleStructTransformation extends SimpleTransformation {

    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(t @ IR.ETuple2(x, y)) =>
        IR.Tuple2SC(x, y)(t.m1, t.m2)
      case Def(ta @ IR.Tuple2Access1(t)) =>
        IR.Field(t, "_1", ta.m)
      case Def(ta @ IR.Tuple2Access2(t)) =>
        IR.Field(t, "_2", ta.m)
      case _ => null
    }
  }

  class TypeTransformations(val map: Map[String, String]) extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(t @ IR.SimpleStruct(x, y)) if map.contains(x.mkString("_")) => {
        val name = x.mkString("_")
        new IR.ObjectCreation(name, y.values.toList)(t.m)
      }
      case _ => null
    }
  }

}
