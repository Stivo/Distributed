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

trait DListOps extends Base with Variables {
  def getArgs = get_args()

  object DList {
    def apply(file: Rep[String]) = dlist_new[String](file)
  }

  implicit def repDListToDListOps[A: Manifest](dlist: Rep[DList[A]]) = new dlistOpsCls(dlist)
  implicit def varDListToDListOps[A: Manifest](dlist: Var[DList[A]]) = new dlistOpsCls(readVar(dlist))
  class dlistOpsCls[A: Manifest](dlist: Rep[DList[A]]) {
    def flatMap[B: Manifest](f: Rep[A] => Rep[Iterable[B]]) = dlist_flatMap(dlist, f)
    def map[B: Manifest](f: Rep[A] => Rep[B]) = dlist_map(dlist, f)
    def filter(f: Rep[A] => Rep[Boolean]) = dlist_filter(dlist, f)
    def save(path: Rep[String]) = dlist_save(dlist, path)
    def ++(dlist2: Rep[DList[A]]) = dlist_++(dlist, dlist2)
  }

  implicit def repDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) = new dlistIterableTupleOpsCls(x)
  implicit def varDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Var[DList[(K, Iterable[V])]]) = new dlistIterableTupleOpsCls(readVar(x))
  class dlistIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) {
    def reduce(f: (Rep[V], Rep[V]) => Rep[V]) = dlist_reduce[K, V](x, f)
  }

  implicit def repDListToDListTupleOps[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) = new dlistTupleOpsCls(x)
  implicit def varDListToDListTupleOps[K: Manifest, V: Manifest](x: Var[DList[(K, V)]]) = new dlistTupleOpsCls(readVar(x))
  class dlistTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) {
    def groupByKey = dlist_groupByKey[K, V](x)
    def join[V2: Manifest](right: Rep[DList[(K, V2)]]) = dlist_join(x, right)
  }

  def get_args(): Rep[Array[String]]

  //operations
  def dlist_new[A: Manifest](file: Rep[String]): Rep[DList[String]]
  def dlist_map[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[B]): Rep[DList[B]]
  def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]): Rep[DList[B]]
  def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Boolean]): Rep[DList[A]]
  def dlist_save[A: Manifest](dlist: Rep[DList[A]], path: Rep[String]): Rep[Unit]
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

trait DListOpsExp extends DListOpsExpBase with DListBaseExp with FunctionsExp {
  def toAtom2[T: Manifest](d: Def[T])(implicit ctx: SourceContext): Exp[T] = super.toAtom(d)

  trait DListNode {
    val directFieldReads = mutable.HashSet[FieldRead]()
    val successorFieldReads = mutable.HashSet[FieldRead]()
    val metaInfos = mutable.Map[String, Any]()
  }

  trait ClosureNode[A, B] extends DListNode {
    val in: Exp[DList[_]]
    def closure: Exp[A => B] = null
    def getClosureTypes: (Manifest[A], Manifest[B])
  }

  trait Closure2Node[A, B, C] extends DListNode {
    val in: Exp[DList[_]]
    def getClosureTypes: ((Manifest[A], Manifest[B]), Manifest[C])

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
      extends Def[DList[B]] with ComputationNodeTyped[DList[A], DList[B]] with ClosureNode[A, B] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getClosureTypes = (mA, mB)
    override def closure = func
    def getTypes = (makeDListManifest[A], makeDListManifest[B])
  }

  case class DListFilter[A: Manifest](in: Exp[DList[A]], func: Exp[A => Boolean])
      extends Def[DList[A]] with PreservingTypeComputation[DList[A]] with ClosureNode[A, Boolean] {
    val mA = manifest[A]
    def getClosureTypes = (mA, Manifest.Boolean)
    def getType = makeDListManifest[A]
    override def closure = func
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

  case class DListGroupByKey[K: Manifest, V: Manifest](dlist: Exp[DList[(K, V)]]) extends Def[DList[(K, Iterable[V])]]
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

  case class DListSave[A: Manifest](dlist: Exp[DList[A]], path: Exp[String]) extends Def[Unit]
      with ComputationNodeTyped[DList[A], Nothing] {
    val mA = manifest[A]
    def getTypes = (manifest[DList[A]], manifest[Nothing])
  }

  case class GetArgs() extends Def[Array[String]]

  case class Narrowing(struct: DList[Rep[SimpleStruct[_]]], fields: List[String]) extends Def[DList[Rep[SimpleStruct[_]]]]

  override def get_args() = GetArgs()
  override def dlist_new[A: Manifest](file: Exp[String]) = NewDList[A](file)
  override def dlist_map[A: Manifest, B: Manifest](dlist: Exp[DList[A]], f: Exp[A] => Exp[B]) = DListMap[A, B](dlist, doLambda(f))
  override def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]) = DListFlatMap(dlist, f)
  override def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Exp[A] => Exp[Boolean]) = DListFilter(dlist, doLambda(f))
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
      case (x: DListNode, y: DListNode) => copyMetaInfoHere(x, y)
      case (x: DListNode, Def(y: DListNode)) => copyMetaInfoHere(x, y)
      case _ =>
    }
  }

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = {
    var out = e match {
      case GetArgs() => GetArgs()
      case vm @ NewDList(path) => NewDList(f(path))(vm.mA)
      case vm @ DListMap(dlist, func) => DListMap(f(dlist), f(func))(vm.mA, vm.mB)
      case vm @ DListFilter(dlist, func) => DListFilter(f(dlist), f(func))(vm.mA)
      case vs @ DListSave(dlist, path) => DListSave(f(dlist), f(path))(vs.mA)
      case v @ DListJoin(left, right) => DListJoin(f(left), f(right))(v.mK, v.mV1, v.mV2)
      case _ => super.mirrorDef(e, f)
    }
    copyMetaInfo(e, out)
    out.asInstanceOf[Def[A]]
  }

  /*
  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = {
    var out = e match {
      case o @ ObjectCreation(name, fields) => toAtom(ObjectCreation(name, fields.mapValues(f(_)))(o.mA))(o.mA)
      case flat @ DListFlatten(list) => toAtom(DListFlatten(f(list))(flat.mA))
      case vm @ NewDList(dlist) => toAtom(NewDList(f(dlist))(vm.mA))(mtype(vm.mA))
      case vm @ DListMap(dlist, func) => toAtom(
        new { override val overrideClosure = Some(f(vm.closure)) } with DListMap(f(dlist), f(func))(vm.mA, vm.mB)
      )(vm.getTypes._2)
      case vf @ DListFilter(dlist, func) => toAtom(
        new { override val overrideClosure = Some(f(vf.closure)) } with DListFilter(f(dlist), f(func))(vf.mA)
      )(mtype(manifest[A]))
      case vfm @ DListFlatMap(dlist, func) => toAtom(
        new { override val overrideClosure = Some(f(vfm.closure)) } with DListFlatMap(f(dlist), f(func))(vfm.mA, vfm.mB)
      )(mtype(manifest[A]))
      case gbk @ DListGroupByKey(dlist) => toAtom(DListGroupByKey(f(dlist))(gbk.mKey, gbk.mValue))(mtype(manifest[A]))
      case v @ DListJoin(left, right) => toAtom(DListJoin(f(left), f(right))(v.mK, v.mV1, v.mV2))(mtype(manifest[A]))
      case v @ DListReduce(dlist, func) => toAtom(
        new { override val overrideClosure = Some(f(v.closure)) } with DListReduce(f(dlist), f(func))(v.mKey, v.mValue)
      )(mtype(manifest[A]))
      case vs @ DListSave(dlist, path) => toAtom(DListSave(f(dlist), f(path))(vs.mA))
      case Reflect(vs @ DListSave(dlist, path), u, es) => reflectMirrored(Reflect(DListSave(f(dlist), f(path))(vs.mA), mapOver(f, u), f(es)))
      case Reify(x, u, es) => toAtom(Reify(f(x), mapOver(f, u), f(es)))(mtype(manifest[A]))
      case _ => super.mirror(e, f)
    }
    copyMetaInfo(e, out)
    out.asInstanceOf[Exp[A]]
  }
  */
  /*
  override def syms(e: Any): List[Sym[Any]] = e match {
    case s: ClosureNode[_, _] => syms(s.in, s.closure)
    case s: Closure2Node[_, _, _] => syms(s.in, s.closure)
    case DListFlatten(x) => syms(x)
    case NewDList(arg) => syms(arg)
    case DListSave(dlist, path) => syms(dlist, path)
    case ObjectCreation(_, fields) => syms(fields)
    case DListJoin(left, right) => syms(left, right)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    // TODO: ++ does not work anymore, because it is defined in ListOps
    case s: ClosureNode[_, _] => freqHot(s.closure, s.in) //++ freqNormal(s.in)
    case s: Closure2Node[_, _, _] => freqHot(s.closure, s.in) //++ freqNormal(s.in)
    case DListFlatten(x) => freqNormal(x)
    case NewDList(arg) => freqNormal(arg)
    case DListSave(dlist, path) => freqNormal(dlist, path)
    case ObjectCreation(_, fields) => freqNormal(fields)
    case DListJoin(left, right) => freqNormal(left, right)
    case _ => super.symsFreq(e)
  }
*/
}

trait DListImplOps extends DListOps with FunctionsExp {

}

trait AbstractScalaGenDList extends ScalaGenBase with DListBaseCodeGenPkg {
  val IR: DListOpsExp
  import IR.{ TP, Stm, SimpleStruct, Def, Sym, Exp, Block, StructTag, ClassTag }
  import IR.{ findDefinition, syms, infix_rhs }
  class BlockVisitor(block: Block[_]) {
    def visitAll(inputSym: Exp[Any]): List[Stm] = {
      def getInputs(x: Exp[Any]) = x match {
        case x: Sym[_] =>
          findDefinition(x) match {
            case Some(x) => syms(infix_rhs(x))
            case None => Nil
          }
        case _ => Nil
      }

      var out = List[Stm]()
      val inputs = getInputs(inputSym)
      for (input <- inputs) input match {
        case s: Sym[_] => {
          val stm = findDefinition(s)
          out ++= (visitAll(s) ++ stm)
        }
        case _ =>
      }
      out.distinct
    }

    lazy val statements = visitAll(block.res)
    lazy val defs = statements.flatMap(_.defs)
  }

  class TypeHandler(block: Block[_]) extends BlockVisitor(block) {
    trait PartInfo[A] {
      def m: Manifest[A]
      def niceName: String
    }
    case class FieldInfo[A: Manifest](val name: String, val niceType: String, position: Int) extends PartInfo[A] {
      val m = manifest[A]
      def niceName = niceType
      //      lazy val containingType = typeInfos2.map(_._2).filter(_.fields.size > position).find(_.fields(position) == this).get
      def getType = typeInfos2(niceType)
    }
    case class TypeInfo[A: Manifest](val name: String, val fields: List[FieldInfo[_]]) extends PartInfo[A] {
      val m = manifest[A]
      def getField(field: String) = fields.find(_.name == field)
      def niceName = name
    }
    val objectCreations = statements.flatMap {
      case TP(_, s @ SimpleStruct(tag, elems)) => Some(s)
      //case TTP(_, ThinDef(s @ SimpleStruct(tag, elems))) => Some(s)
      case _ => None
    }

    def getNameForTag(t: StructTag[_]) = t match {
      case ClassTag(n) => n
      case _ => throw new RuntimeException("Add name for this tag type")
    }

    val remappings = objectCreations.map {
      s =>
        (s.m, getNameForTag(s.tag))
    }.toMap
    def cleanUpType(m: Manifest[_]) = {
      var out = m.toString
      remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))
      out
    }
    // Phi's do not have the correct type.
    //    def getType(s: Exp[_]) = s match {
    //      case Def(Phi(_, _, _, _, x)) => x.Type
    //      case x => x.Type
    //    }
    val typeInfos = objectCreations.map {
      s =>
        (s.tag, s.elems) //.mapValues(x => cleanUpType(getType(x))))
    }.toMap
    val typeInfos2 = objectCreations.map {
      s =>
        var i = -1
        val name = getNameForTag(s.tag)
        val fields = s.elems.map { x =>
          i += 1;
          val typ = x._2.tp
          new FieldInfo(x._1, cleanUpType(typ), i)(typ)
        }.toList
        (name, new TypeInfo(name, fields)(s.m))
    }.toMap

    def getTypeAt(path: String, mIn: Manifest[_]): PartInfo[_] = {
      //      println()
      //      println("#### "+path+" in "+mIn)
      val pathParts = path.split("\\.").drop(1).toList
      val m = mIn.asInstanceOf[Manifest[Any]]
      var typeNow: Any = m
      var restPath = pathParts
      val step1 = mIn match {
        // if m is a tuple:
        case x: Manifest[(_, _)] if (x.toString.startsWith("scala.Tuple2")) =>
          pathParts match {
            case Nil =>
              val f1 = new FieldInfo("_1", cleanUpType(x.typeArguments(0)), 0)(x.typeArguments(0))
              val f2 = new FieldInfo("_2", cleanUpType(x.typeArguments(1)), 1)(x.typeArguments(1))
              new TypeInfo("tuple2s", f1 :: f2 :: Nil)(x)
            case "_1" :: _ => {
              restPath = restPath.drop(1)
              new FieldInfo("_1", cleanUpType(x.typeArguments(0)), 0)(x.typeArguments(0))
            }
            case "_2" :: _ => {
              restPath = restPath.drop(1)
              new FieldInfo("_2", cleanUpType(x.typeArguments(1)), 1)(x.typeArguments(1))
            }
          }
        // if m is a normal type: just look up the type for this manifest
        case x => typeInfos2(cleanUpType(x))
      }

      //      println("Step 1"+step1+", rest is "+restPath)
      def getRest(restPath: List[String], x: PartInfo[_]): PartInfo[_] = {
        //        println("Looking up rest of the path "+restPath+" for "+x)
        restPath match {
          case Nil => x
          case field :: _ =>
            val typeInfo = x match {
              case f: FieldInfo[_] => typeInfos2(f.niceType)
              case f: TypeInfo[_] => f
            }
            getRest(restPath.drop(1), typeInfo.getField(field).get)
        }
      }

      val out = getRest(restPath, step1)
      //      println("----- Returning "+out)
      //      println()
      out

    }

  }

  var typeHandler: TypeHandler = null

  override def remap[A](m: Manifest[A]): String = {
    val remappings = typeHandler.remappings.filter(!_._2.startsWith("tuple2s"))
    var out = super.remap[A](m)
    if (out.startsWith("ch.epfl.distributed.DList")) {
      out = out.substring("ch.epfl.distributed.".length)
    }
    remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))

    // hack for problem with constant tuples in nested tuples
    val expname = "Expressions$Exp["
    while (out.contains(expname)) {
      println("##*$*%**%%** => Remaphack in progress for " + out)
      val start = out.indexOf(expname) + expname.length
      val end = out.indexOf("]", start)
      val len = end - start
      val actualType = out.substring(start, end)
      val searchStart = out.substring(0, start)
      val removeLength = expname.length +
        searchStart.reverse.drop(expname.length)
        .takeWhile { x => x != '[' && x != ' ' }.length
      val (before, after) = out.splitAt(start)
      out = before.reverse.drop(removeLength).reverse + actualType + after.drop(actualType.length + 1)
    }
    out
  }

  def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)]

}

trait ScalaGenDList extends AbstractScalaGenDList with Matchers with DListTransformations with DListAnalysis {
  val IR: DListOpsExp
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
  import IR.{ SimpleStruct }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case nv @ NewDList(filename) => emitValDef(sym, "New dlist created from %s with type %s".format(filename, nv.mA))
    case vs @ DListSave(dlist, filename) => stream.println("Saving dlist %s (of type %s) to %s".format(dlist, remap(vs.mA), filename))
    case vm @ DListMap(dlist, func) => emitValDef(sym, "mapping dlist %s with function %s, type %s => %s".format(dlist, quote(func), vm.mA, vm.mB))
    //    case vf @ DListFilter(dlist, function) => emitValDef(sym, "filtering dlist %s with function %s".format(dlist, function))
    //    case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "flat mapping dlist %s with function %s".format(dlist, function))
    //    case vm @ DListFlatten(v1) => emitValDef(sym, "flattening dlists %s".format(v1))
    //    case gbk @ DListGroupByKey(dlist) => emitValDef(sym, "grouping dlist by key")
    //    case gbk @ DListJoin(left, right) => emitValDef(sym, "Joining %s with %s".format(left, right))
    //    case red @ DListReduce(dlist, f) => emitValDef(sym, "reducing dlist")
    case GetArgs() => emitValDef(sym, "getting the arguments")
    case IR.Lambda(_, _, _) if inlineClosures =>
    case IR.Lambda2(_, _, _, _) if inlineClosures =>
    case _ => super.emitNode(sym, rhs)
  }

  def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {

    val x = fresh[A]
    val y = reifyBlock(f(x))

    typeHandler = new TypeHandler(y)

    val sA = remap(mA)
    val sB = remap(mB)

    withStream(stream) {
      stream.println("/*****************************************\n" +
        "  Emitting Generated Code                  \n" +
        "*******************************************/")

      // TODO: separate concerns, should not hard code "pxX" name scheme for static data here
      //      stream.println("class "+className+" extends (("+sA+")=>("+sB+")) {")
      //      stream.println("def apply("+quote(x)+":"+sA+"): "+sB+" = {")

      emitBlock(y)
      stream.println(quote(getBlockResult(y)))

      //      stream.println("}")
      //    
      //      stream.println("}")
      //      stream.println("/*****************************************\n"+
      //                     "  End of Generated Code                  \n"+
      //                     "*******************************************/")
    }

    Nil
  }

  /*
  override def fattenAll(e: List[TP[Any]]): List[TTP] = {
    val out = super.fattenAll(e)
    if (!typeHandler.isInstanceOf[TypeHandler] || hasDListNodes(out)) {
      typeHandler = new TypeHandler(out)
    }
    out
  }
  */

  def writeClosure(closure: Exp[_]) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    def remapHere(x: Manifest[_]) = if (typesInInlinedClosures) ": " + remap(x) else ""
    withStream(pw) {
      closure match {
        case Def(Lambda(fun, x, y)) => {
          pw.println("{ %s %s => ".format(quote(x), remapHere(x.tp)))
          emitBlock(y)
          pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
          pw.print("}")
        }
        case Def(Lambda2(fun, x1, x2, y)) => {
          pw.println("{ (%s %s, %s %s) => ".format(quote(x1), remapHere(x1.tp), quote(x2), remapHere(x2.tp)))
          emitBlock(y)
          pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
          pw.print("}")
        }
      }
    }
    pw.flush
    sw.toString
  }

  def inlineClosures = false

  def typesInInlinedClosures = false

  def handleClosure(closure: Exp[_]) = {
    if (inlineClosures) {
      writeClosure(closure)
    } else {
      quote(closure)
    }
  }

  var narrowExistingMaps = true
  var insertNarrowingMaps = true
  var mapMerge = true
  /*
  def mapNarrowing(transformer: Transformer) {
    // replace maps with narrower ones
    var oneFound = false
    var pullDeps = newPullDeps
    if (narrowExistingMaps) {
      do {
        oneFound = false
        // perform field usage analysis
        val analyzer = newAnalyzer(transformer.currentState, typeHandler)
        analyzer.makeFieldAnalysis
        var goOn = true
        analyzer.ordered.foreach {
          case _ if !goOn =>
          case v @ DListMap(in, func) if !v.metaInfos.contains("narrowed")
            && !SimpleType.unapply(v.getClosureTypes._2).isDefined
            && analyzer.hasObjectCreationInClosure(v) => {
            oneFound = true
            v.metaInfos += (("narrowed", true))
            // transformer.currentState.printAll("Before narrowing")
            transformer.doTransformation(new MapNarrowTransformationNew(v, typeHandler), 50)
            transformer.doTransformation(pullDeps, 500)
            // transformer.currentState.printAll("After narrowing")
            transformer.doTransformation(new FieldOnStructReadTransformation, 500)
            goOn = false
          }
          case _ =>
        }
      } while (oneFound)
    }

  }

  def insertNarrowingMaps(transformer: Transformer) {
    var oneFound = false
    var pullDeps = newPullDeps
    if (insertNarrowingMaps) {
      //inserting narrowing dlistmaps where analyzer says it should 
      do {
        oneFound = false
        val analyzer = newAnalyzer(transformer.currentState, typeHandler)
        analyzer.makeFieldAnalysis
        for (x <- analyzer.narrowBefore) {
          if (!oneFound) {
            pullDeps = newPullDeps
            val increase = x.metaInfos.getOrElse("insertedNarrowers", 0).asInstanceOf[Int]

            analyzer.getInputs(x).foreach { input =>
              x.metaInfos("insertedNarrowers") = 1 + increase
              val inserter = new InsertMapNarrowTransformation(input, x.directFieldReads.toList)
              transformer.doTransformation(inserter, 1)
              inserter.lastOut match {
                case None =>
                case Some(narrowThis) =>
                  transformer.doTransformation(pullDeps, 500)
                  val analyzer2 = newAnalyzer(transformer.currentState, typeHandler)
                  analyzer2.makeFieldAnalysis
                  transformer.doTransformation(new MapNarrowTransformationNew(narrowThis, typeHandler), 2)
                  transformer.doTransformation(pullDeps, 500)
                  oneFound = true
              }
            }
          }
        }
      } while (oneFound)
    }

  }

  def transformTree(state: TransformationState): TransformationState = state

  
  override def focusExactScopeFat[A](currentScope0In: List[TTP])(result0B: List[Block[Any]])(body: List[TTP] => A): A = {
    // only do our optimizations in top scope
    if (hasDListNodes(currentScope0In)) {
      // set up state
      var result0 = result0B.map(getBlockResultFull)
      var state = new TransformationState(currentScope0In, result0)

      // transform the tree (backend specific)
      state = transformTree(state)

      // return optimized tree to 
      var currentScope0 = state.ttps
      result0 = state.results
      // hack: should maybe not add all nodes here, but seems to work, as we are in the top scope
      innerScope ++= IR.globalDefs.filter(!innerScope.contains(_))
      currentScope0 = getFatSchedule(currentScope0)(result0)
      super.focusExactScopeFat(currentScope0)(result0.map(IR.Block(_)))(body)
    } else {
      super.focusExactScopeFat(currentScope0In)(result0B)(body)
    }
  }

  def mapNarrowingAndInsert(transformer: Transformer) {
    mapNarrowing(transformer)
    insertNarrowingMaps(transformer)
  }

  
  
  def writeGraphToFile(transformer: Transformer, name: String, comments: Boolean = true) {
    val out = new FileOutputStream(name)
    val analyzer = newAnalyzer(transformer.currentState, typeHandler)
    analyzer.makeFieldAnalysis
    analyzer.addComments = comments
    out.write(analyzer.exportToGraph.getBytes)
    out.close

  }
*/
}

trait TypeFactory extends ScalaGenDList {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  def makeTypeFor(name: String, fields: Iterable[String]): String

  val types = mutable.Map[String, String]()

  val skipTypes = mutable.Set[String]()

  def restTypes = types.filterKeys(x => !skipTypes.contains(x))

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case IR.Field(tuple, x, tp) => emitValDef(sym, "%s.%s".format(quote(tuple), x))
      //case IR.SimpleStruct(tag, elems) => emitValDef(sym, "Creating struct with %s and elems %s".format(tag, elems))
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "%s(%s)".format(typeName, fieldsList.map(_._2).map(quote).mkString(", ")))
        } catch {
          case e => emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

}

trait CaseClassTypeFactory extends TypeFactory {
  def makeTypeFor(name: String, fields: Iterable[String]): String = {

    // fields is a sorted list of the field names
    // typeInfo is the type with all fields and all infos
    val typeInfo = typeHandler.typeInfos2(name)
    // this is just a set to have contains
    val fieldsSet = fields.toSet
    val fieldsInType = typeInfo.fields
    val fieldsHere = typeInfo.fields.filter(x => fieldsSet.contains(x.name))
    if (!types.contains(name)) {
      types(name) = "trait %s extends Serializable {\n%s\n} ".format(name,
        fieldsInType.map {
          fi =>
            """def %s : %s = throw new RuntimeException("Should not try to access %s here, internal error")"""
              .format(fi.name, fi.niceName, fi.name)
        }.mkString("\n"))
    }
    val typeName = name + ((List("") ++ fieldsHere.map(_.position + "")).mkString("_"))
    if (!types.contains(typeName)) {
      val args = fieldsHere.map { fi => "override val %s : %s".format(fi.name, fi.niceName) }.mkString(", ")
      types(typeName) = """case class %s(%s) extends %s {
   override def toString() = {
        val sb = new StringBuilder()
        sb.append("%s(")
        %s
        sb.append(")")
        sb.toString()
   }
}""".format(typeName, args, name, name,
        fieldsInType
          .map(x => if (fieldsSet.contains(x.name)) x.name else "")
          .map(x => """%s sb.append(",")""".format(if (x.isEmpty) "" else "sb.append(%s); ".format(x)))
          .mkString(";\n"))
    }

    typeName
  }

}
