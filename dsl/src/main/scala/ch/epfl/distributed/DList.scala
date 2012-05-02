package ch.epfl.distributed

import scala.virtualization.lms.common._
import scala.virtualization.lms.internal._
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.PrintWriter
import java.io.File
import java.io.FileWriter
import java.io.StringWriter
import scala.reflect.SourceContext
import scala.collection.mutable
import scala.collection.immutable
import java.util.regex.Pattern

trait DList[+A]

trait VectorBase extends Base with LiftAll
  with Equal with IfThenElse with Variables with While with Functions
  with ImplicitOps with NumericOps with OrderingOps with StringOps
  with BooleanOps with PrimitiveOps with MiscOps with TupleOps
  with MathOps with CastingOps with ObjectOps with ArrayOps
  with IterableOps with ListOps
//  with MoreIterableOps
//  with StringAndNumberOps  with DateOps

/*
trait VectorBaseExp extends VectorBase
  with DSLOpsExp with BlockExp
  with EqualExp with IfThenElseExp with VariablesExp with WhileExp with FunctionsExp
  with ImplicitOpsExp with NumericOpsExp with OrderingOpsExp with StringOpsExp with StringOpsExpOpt
  with BooleanOpsExp with PrimitiveOpsExp with MiscOpsExp 
  //with StructTupleOpsExp
  with MathOpsExp with CastingOpsExp with ObjectOpsExp with ArrayOpsExp with RangeOpsExp
  with StructExp with StructExpOpt
  with StructFatExp with StructFatExpOptCommon
  with FatExpressions with LoopsFatExp with IfThenElseFatExp
  with IterableOpsExp with ListOpsExp
//  with StringPatternOpsExp with MoreIterableOpsExp
//  with StringAndNumberOpsExp  with DateOpsExp
*/
  
trait DListBaseExp extends DListOps
  with StructExp with StructExpOpt
  with FunctionsExp
  
/*
trait VectorBaseCodeGenPkg extends ScalaGenDSLOps
  with SimplifyTransform with ScalaGenIfThenElseFat
  with ScalaGenEqual with ScalaGenIfThenElse with ScalaGenVariables with ScalaGenWhile with ScalaGenFunctions
  with ScalaGenImplicitOps with ScalaGenNumericOps with ScalaGenOrderingOps with ScalaGenStringOps
  with ScalaGenBooleanOps with ScalaGenPrimitiveOps with ScalaGenMiscOps with ScalaGenTupleOps
  with ScalaGenMathOps with ScalaGenCastingOps with ScalaGenObjectOps with ScalaGenArrayOps with ScalaGenRangeOps
//  with ScalaGenDateOps
  //with ScalaGenFatStruct
  with ScalaGenStruct with GenericFatCodegen
//  with StringPatternOpsCodeGen with MoreIterableOpsCodeGen
//  with StringAndNumberOpsCodeGen 
  with ScalaGenListOps with ScalaGenIterableOps { val IR: VectorOpsExp }
*/

trait DListOps extends Base with Variables {
  def getArgs = get_args()

  object DList {
    def apply(file: Rep[String]) = dlist_new[String](file)
  }

  implicit def repDlistToDListOps[A: Manifest](dlist: Rep[DList[A]]) = new dListOpsCls[A](dlist)
  implicit def varDlistToDListOps[A: Manifest](dlist: Var[DList[A]]) = new dListOpsCls[A](readVar(dlist))
  class dListOpsCls[A: Manifest](dlist: Rep[DList[A]]) {
//    def flatMap[B: Manifest](f: Rep[A] => Rep[Iterable[B]]) = dlist_flatMap(dlist, f)
    def map[B: Manifest](f: Rep[A] => Rep[B]) = dlist_map(dlist, f)
    def filter(f: Rep[A] => Rep[Boolean]) = dlist_filter(dlist, f)
    def save(path: Rep[String]) = dlist_save(dlist, path)
//    def ++(dlist2: Rep[DList[A]]) = dlist_++(dlist, dlist2)
  }

//  implicit def repVecToVecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) = new vecIterableTupleOpsCls(x)
//  class vecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) {
//    def reduce(f: (Rep[V], Rep[V]) => Rep[V]) = dlist_reduce[K, V](x, f)
//  }
//
//  implicit def repVecToVecTupleOps[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) = new vecTupleOpsCls(x)
//  class vecTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) {
//    def groupByKey = dlist_groupByKey[K, V](x)
//    def join[V2: Manifest](right: Rep[DList[(K, V2)]]) = dlist_join(x, right)
//  }

  def get_args(): Rep[Array[String]]

  //operations
  def dlist_new[A: Manifest](file: Rep[String]): Rep[DList[String]]
  def dlist_map[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[B]): Rep[DList[B]]
  def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Boolean]): Rep[DList[A]]
  def dlist_save[A: Manifest](dlist: Rep[DList[A]], path: Rep[String]): Rep[Unit]
  def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]): Rep[DList[B]]
  def dlist_++[A: Manifest](dlist1: Rep[DList[A]], dlist2: Rep[DList[A]]): Rep[DList[A]]
  def dlist_reduce[K: Manifest, V: Manifest](dlist: Rep[DList[(K, Iterable[V])]], f: (Rep[V], Rep[V]) => Rep[V]): Rep[DList[(K, V)]]
  def dlist_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]]): Rep[DList[(K, (V1, V2))]]
  def dlist_groupByKey[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]]): Rep[DList[(K, Iterable[V])]]
}

object FakeSourceContext {
  def apply() = SourceContext("unknown", Nil)
}

case class FieldRead(val path: String) {
  val getPath = path.split("\\.").toList
}

trait DListOpsExp extends DListOps with DListBaseExp with WhileExp {
  def toAtom2[T: Manifest](d: Def[T])(implicit ctx: SourceContext): Exp[T] = super.toAtom(d)

  trait DListNode {
    val directFieldReads = mutable.HashSet[FieldRead]()
    val successorFieldReads = mutable.HashSet[FieldRead]()
    val metaInfos = mutable.Map[String, Any]()
  }

  trait ClosureNode[A, B] extends DListNode {
    val in: Exp[DList[_]]
    val func: Exp[A] => Exp[B]
    def getClosureTypes: (Manifest[A], Manifest[B])

    val overrideClosure: Option[Exp[A => B]] = None

    lazy val closure: Exp[A => B] = {
      overrideClosure.getOrElse{
          println("Creating new closure ");
        DListOpsExp.this.doLambda(func)(getClosureTypes._1, getClosureTypes._2, FakeSourceContext())
      }
    }
  }

  trait Closure2Node[A, B, C] extends DListNode {
    val in: Exp[DList[_]]
    val func: (Exp[A], Exp[B]) => Exp[C]
    def getClosureTypes: ((Manifest[A], Manifest[B]), Manifest[C])

    val overrideClosure: Option[Exp[(A, B) => C]] = None

    lazy val closure = {
      overrideClosure.getOrElse(
        DListOpsExp.this.doLambda2(func)(getClosureTypes._1._1, getClosureTypes._1._2, getClosureTypes._2, FakeSourceContext())
      )
    }
  }

  trait ComputationNode extends DListNode {
    def getTypes: (Manifest[_], Manifest[_])
    def getElementTypes: (Manifest[_], Manifest[_]) = (getTypes._1.typeArguments(0), getTypes._2.typeArguments(0))
  }

  trait ComputationNodeTyped[A, B] extends ComputationNode {
    override def getTypes: (Manifest[A], Manifest[B])
  }

  trait PreservingTypeComputation[A] extends ComputationNodeTyped[A, A] {
    def getType: Manifest[A]
    def getTypes = (getType, getType)
  }

  case class NewDList[A: Manifest](file: Exp[String]) extends Def[DList[String]]
      with ComputationNodeTyped[Nothing, DList[A]] {
    val mA = manifest[A]
    def getTypes = (manifest[Nothing], manifest[DList[A]])
  }

  def makeDListManifest[B: Manifest] = manifest[DList[B]]

  case class DListMap[A: Manifest, B: Manifest](in: Exp[DList[A]], func: Exp[A => B])
      extends Def[DList[B]] with ComputationNodeTyped[DList[A], DList[B]]  {
    val mA = manifest[A]
    val mB = manifest[B]
    def getClosureTypes = (mA, mB)
    def getTypes = (makeDListManifest[A], makeDListManifest[B])
  }

  case class DListFilter[A: Manifest](in: Exp[DList[A]], func: Exp[A] => Exp[Boolean])
      extends Def[DList[A]] with PreservingTypeComputation[DList[A]] with ClosureNode[A, Boolean] {
    val mA = manifest[A]
    def getClosureTypes = (mA, Manifest.Boolean)
    def getType = makeDListManifest[A]
  }

  case class DListFlatMap[A: Manifest, B: Manifest](in: Exp[DList[A]], func: Exp[A] => Exp[Iterable[B]])
      extends Def[DList[B]] with ComputationNodeTyped[DList[A], DList[B]] with ClosureNode[A, Iterable[B]] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getTypes = (manifest[DList[A]], manifest[DList[B]])
    def getClosureTypes = (manifest[A], manifest[Iterable[B]])
  }

  case class DListFlatten[A: Manifest](dlists: List[Exp[DList[A]]]) extends Def[DList[A]]
      with PreservingTypeComputation[DList[A]] {
    val mA = manifest[A]
    def getType = manifest[DList[A]]
  }

  case class DListGroupByKey[K: Manifest, V: Manifest](v1: Exp[DList[(K, V)]]) extends Def[DList[(K, Iterable[V])]]
      with ComputationNodeTyped[DList[(K, V)], DList[(K, Iterable[V])]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    val mOutType = manifest[(K, Iterable[V])]
    val mInType = manifest[(K, V)]
    def getTypes = (manifest[DList[(K, V)]], manifest[DList[(K, Iterable[V])]])
  }

  case class DListReduce[K: Manifest, V: Manifest](in: Exp[DList[(K, Iterable[V])]], func: (Exp[V], Exp[V]) => Exp[V])
      extends Def[DList[(K, V)]] with Closure2Node[V, V, V]
      with ComputationNodeTyped[DList[(K, Iterable[V])], DList[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getTypes = (manifest[DList[(K, Iterable[V])]], manifest[DList[(K, V)]])
  }

  case class DListJoin[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[DList[(K, V1)]], right: Exp[DList[(K, V2)]])
      extends Def[DList[(K, (V1, V2))]] with DListNode {
    def mK = manifest[K]
    def mV1 = manifest[V1]
    def mV2 = manifest[V2]
    def mIn1 = manifest[(K, V1)]
  }

  case class DListSave[A: Manifest](dlists: Exp[DList[A]], path: Exp[String]) extends Def[Unit]
      with ComputationNodeTyped[DList[A], Nothing] {
    val mA = manifest[A]
    def getTypes = (manifest[DList[A]], manifest[Nothing])
  }

  case class GetArgs() extends Def[Array[String]]

  case class Narrowing(struct: DList[Rep[SimpleStruct[_]]], fields: List[String]) extends Def[DList[Rep[SimpleStruct[_]]]]

  case class ObjectCreation[A: Manifest](className: String, fields: Map[String, Rep[_]]) extends Def[A] {
    val mA = manifest[A]
  }

  override def get_args() = GetArgs()
  override def dlist_new[A: Manifest](file: Exp[String]) = NewDList[A](file)
  override def dlist_map[A: Manifest, B: Manifest](dlist: Exp[DList[A]], f: Exp[A] => Exp[B]) = DListMap[A, B](dlist, doLambda(f))
  override def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]) = DListFlatMap(dlist, f)
  override def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Exp[A] => Exp[Boolean]) = DListFilter(dlist, f)
  override def dlist_save[A: Manifest](dlist: Exp[DList[A]], file: Exp[String]) = {
    val save = new DListSave[A](dlist, file)
    reflectEffect(save)
  }
  override def dlist_++[A: Manifest](dlist1: Rep[DList[A]], dlist2: Rep[DList[A]]) = DListFlatten(immutable.List(dlist1, dlist2))
  override def dlist_reduce[K: Manifest, V: Manifest](dlist: Exp[DList[(K, Iterable[V])]], f: (Exp[V], Exp[V]) => Exp[V]) = DListReduce(dlist, f)
  override def dlist_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]]): Rep[DList[(K, (V1, V2))]] = DListJoin(left, right)
  override def dlist_groupByKey[K: Manifest, V: Manifest](dlist: Exp[DList[(K, V)]]) = DListGroupByKey(dlist)

  def copyMetaInfo(from: Any, to: Any) = {
    def copyMetaInfoHere[A <: DListNode](from: DListNode, to: A) = { to.metaInfos ++= from.metaInfos; to }
    (from, to) match {
      case (x: DListNode, Def(y: DListNode)) => copyMetaInfoHere(x, y)
      case (Def(x: DListNode), Def(y: DListNode)) => copyMetaInfoHere(x, y)
      case _ =>
    }
  }

  override def mirrorDef[A:Manifest](e: Def[A], f: Transformer)(implicit ctx: SourceContext): Def[A] = {
//    println("Calling mirror with "+e)
    var out = e match {
      case GetArgs() => GetArgs()
      case o @ ObjectCreation(name, fields) => ObjectCreation(name, fields.mapValues(f(_)))(o.mA)
//      case flat @ DListFlatten(list) => toAtom(DListFlatten(f(list))(flat.mA))
      case vm @ NewDList(dlist) => NewDList(f(dlist))(vm.mA)
      case vm @ DListMap(dlist, func) => new DListMap(f(dlist), f(func))(vm.mA, vm.mB)
//        new { override val overrideClosure = Some(f(vm.closure)) } with DListMap(f(dlist), f(func))(vm.mA, vm.mB)
      
      case vf @ DListFilter(dlist, func) => 
        new { override val overrideClosure = Some(f(vf.closure)) } with DListFilter(f(dlist), f(func))(vf.mA)
//      case vfm @ DListFlatMap(dlist, func) => toAtom(
//        new { override val overrideClosure = Some(f(vfm.closure)) } with DListFlatMap(f(dlist), f(func))(vfm.mA, vfm.mB)
//      )(mtype(manifest[A]))
//      case gbk @ DListGroupByKey(dlist) => toAtom(DListGroupByKey(f(dlist))(gbk.mKey, gbk.mValue))(mtype(manifest[A]))
//      case v @ DListJoin(left, right) => toAtom(DListJoin(f(left), f(right))(v.mK, v.mV1, v.mV2))(mtype(manifest[A]))
//      case v @ DListReduce(dlist, func) => toAtom(
//        new { override val overrideClosure = Some(f(v.closure)) } with DListReduce(f(dlist), f(func))(v.mKey, v.mValue)
//      )(mtype(manifest[A]))
      case vs @ DListSave(dlist, path) => DListSave(f(dlist), f(path))
//      case Reflect(vs @ DListSave(dlist, path), u, es) => Reflect(DListSave(f(dlist), f(path))(vs.mA), mapOver(f, u), f(es))
//      case While(cond, block) => While(f(cond), f(block))
//      case Reflect(While(cond, block), u, es) => Reflect(While(f(cond), f(block)), mapOver(f, u), f(es))
 
      case Reify(x, u, es) => Reify(f(x), mapOver(f, u), f(es))
      case _ => super.mirrorDef(e, f)
    }
    copyMetaInfo(e, out)
    
    out.asInstanceOf[Def[A]]
  }
  
/*  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = e match {
//    case Reflect(While(cond,block), u, es) => reflectMirrored(Reflect(While(f(cond),f(block)), mapOver(f,u), f(es))).asInstanceOf[Exp[A]]
    case While(cond, block) => (
      if (f.hasContext)
        While(Block(f.reflectBlock(cond)),Block(f.reflectBlock(block)))
      else
        While(f(cond),f(block)) // FIXME: should apply pattern rewrites (ie call smart constructor)
        ).asInstanceOf[Exp[A]]
    case _ => super.mirror(e,f)
  }
*/
  
  override def syms(e: Any): List[Sym[Any]] = e match {
//    case s: ClosureNode[_, _] => syms(s.in, s.closure)
//    case s: Closure2Node[_, _, _] => syms(s.in, s.closure)
    case DListFlatten(x) => syms(x)
//    case NewDList(arg) => syms(arg)
//    case DListSave(vec, path) => syms(vec, path)
    case ObjectCreation(_, fields) => syms(fields)
//    case DListJoin(left, right) => syms(left, right)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    // TODO: ++ does not work anymore, because it is defined in ListOps
//    case s: ClosureNode[_, _] => freqHot(s.closure, s.in) //++ freqNormal(s.in)
//    case s: Closure2Node[_, _, _] => freqHot(s.closure, s.in) //++ freqNormal(s.in)
    case DListFlatten(x) => freqNormal(x)
//    case NewDList(arg) => freqNormal(arg)
//    case DListSave(vec, path) => freqNormal(vec, path)
    case ObjectCreation(_, fields) => freqNormal(fields)
//    case DListJoin(left, right) => freqNormal(left, right)
    case _ => super.symsFreq(e)
  }

//  override def boundSyms(e: Any): List[Sym[Any]] = e match {
//   case s: ClosureNode[_, _] => effectSyms(s.in, s.closure) //s.in.asInstanceOf[Sym[_]] :: effectSyms(s.closure) //++ freqNormal(s.in)
//    case s: Closure2Node[_, _, _] => syms(s.in, s.closure) // s.in.asInstanceOf[Sym[_]] :: effectSyms(s.closure) //++ freqNormal(s.in)
//    case _ => super.boundSyms(e)
//  }  

  
}

trait PrinterGenDList extends ScalaNestedCodegen {
  val IR: DListOpsExp with FunctionsExp
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
    DListJoin,
    GetArgs
  }
  import IR.{Lambda, Lambda2}

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case nv @ NewDList(filename) => emitValDef(sym, "New vector created from %s with type %s".format(filename, nv.mA))
    case vs @ DListSave(vector, filename) => stream.println("Saving vector %s (of type %s) to %s".format(vector, vs.mA, filename))
    case vm @ DListMap(vector, function) => emitValDef(sym, "mapping vector %s with function %s, type %s => %s".format(vector, quote(vm.func), vm.mA, vm.mB))
    case vf @ DListFilter(vector, function) => emitValDef(sym, "filtering vector %s with function %s".format(vector, quote(vf.closure)))
    case vm @ DListFlatMap(vector, function) => emitValDef(sym, "flat mapping vector %s with function %s".format(vector, function))
    case vm @ DListFlatten(v1) => emitValDef(sym, "flattening vectors %s".format(v1))
    case gbk @ DListGroupByKey(vector) => emitValDef(sym, "grouping vector by key")
    case gbk @ DListJoin(left, right) => emitValDef(sym, "Joining %s with %s".format(left, right))
    case red @ DListReduce(vector, f) => emitValDef(sym, "reducing vector")
    case GetArgs() => emitValDef(sym, "getting the arguments")
//    case IR.Lambda(_, _, _) if inlineClosures =>
//    case IR.Lambda2(_, _, _, _) if inlineClosures =>
    case _ => super.emitNode(sym, rhs)

  }
  
//  val typesInInlinedClosures = false
//  def writeClosure(closure: Exp[_]) = {
//    val sw = new StringWriter()
//    val pw = new PrintWriter(sw)
//    def remapHere(x: Manifest[_]) = if (typesInInlinedClosures) ": " + remap(x) else ""
//    closure match {
//      case Def(Lambda(fun, x, y)) => {
//        pw.println("{ %s %s => ".format(quote(x), remapHere(x.tp)))
//        emitBlock(y)
//        pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
//        pw.print("}")
//      }
//      case Def(Lambda2(fun, x1, x2, y)) => {
//        pw.println("{ (%s %s, %s %s) => ".format(quote(x1), remapHere(x1.tp), quote(x2), remapHere(x2.tp)))
//        emitBlock(y)
//        pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
//        pw.print("}")
//      }
//    }
//    pw.flush
//    sw.toString
//  }

    override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, streamIn: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    //    val func : Exp[A] => Exp[B] = {x => reifyEffects(f(x))}

    val capture = new StringWriter
    val stream = new PrintWriter(capture)

    val x = IR.fresh[A]
    val y = reifyBlock(f(x))

    val sA = mA.toString
    val sB = mB.toString

    //    val staticData = getFreeDataBlock(y)
    
    emitBlock(y)

//    stream.println(restTypes.values.toList.sorted.mkString("\n"))

    stream.flush
    val out = capture.toString
//    val newOut = out.replace("###wireFormats###", mkWireFormats)
    //    staticData
//    streamIn.print(newOut)
    Nil
  }


}