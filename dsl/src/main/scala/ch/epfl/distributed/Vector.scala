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

trait Vector[+A]

trait Tuple2Struct extends Base with Expressions {
  class Tuple2S[X: Manifest, Y: Manifest]
  //  def Tuple2SC[X : Manifest,Y : Manifest](x : Rep[X], y : Rep[Y]) : Rep[Tuple2S[X,Y]]
  //  def infix__1[X : Manifest](c: Rep[Tuple2S[X,_]]): Rep[X]
  //  def infix__2[X : Manifest](c: Rep[Tuple2S[_,X]]): Rep[X]
}

trait Tuple2StructExp extends Tuple2Struct with StructExp {
  def Tuple2SC[X: Manifest, Y: Manifest](x: Rep[X], y: Rep[Y]) =
    SimpleStruct[Tuple2[X, Y]](List("tuple2s"), Map("_1" -> x, "_2" -> y))
  //  def infix__1[X : Manifest](c: Rep[Tuple2S[X,_]]): Rep[X] = field[X](c, "_1")
  //  def infix__2[X : Manifest](c: Rep[Tuple2S[_,X]]): Rep[X] = field[X](c, "_2")
}

trait VectorBase extends Base with LiftAll
  with Equal with IfThenElse with Variables with While with Functions
  with ImplicitOps with NumericOps with OrderingOps with StringOps
  with BooleanOps with PrimitiveOps with MiscOps with TupleOps
  with MathOps with CastingOps with ObjectOps with ArrayOps
  with StringAndNumberOps with ListOps with Tuple2Struct

trait VectorBaseExp extends VectorBase
  with DSLOpsExp with Tuple2StructExp with BlockExp
  with EqualExp with IfThenElseExp with VariablesExp with WhileExp with FunctionsExp
  with ImplicitOpsExp with NumericOpsExp with OrderingOpsExp with StringOpsExp
  with BooleanOpsExp with PrimitiveOpsExp with MiscOpsExp with TupleOpsExp
  with MathOpsExp with CastingOpsExp with ObjectOpsExp with ArrayOpsExp with RangeOpsExp
  with StructExp with FatExpressions with LoopsFatExp with IfThenElseFatExp
  with StringAndNumberOpsExp with StructFatExpOptCommon with ListOpsExp

trait VectorBaseCodeGenPkg extends ScalaGenDSLOps
  with SimplifyTransform with ScalaGenIfThenElseFat
  with ScalaGenEqual with ScalaGenIfThenElse with ScalaGenVariables with ScalaGenWhile with ScalaGenFunctions
  with ScalaGenImplicitOps with ScalaGenNumericOps with ScalaGenOrderingOps with ScalaGenStringOps
  with ScalaGenBooleanOps with ScalaGenPrimitiveOps with ScalaGenMiscOps with ScalaGenTupleOps
  with ScalaGenMathOps with ScalaGenCastingOps with ScalaGenObjectOps with ScalaGenArrayOps with ScalaGenRangeOps
  with ScalaGenFatStruct with GenericFatCodegen
  with StringAndNumberOpsCodeGen with ScalaGenListOps { val IR: VectorOpsExp }

trait VectorOps extends VectorBase {
  def getArgs = get_args()

  object Vector {
    def apply(file: Rep[String]) = vector_new[String](file)

  }

  implicit def repVecToVecOps[A: Manifest](vector: Rep[Vector[A]]) = new vecOpsCls(vector)
  class vecOpsCls[A: Manifest](vector: Rep[Vector[A]]) {
    def flatMap[B: Manifest](f: Rep[A] => Rep[Iterable[B]]) = vector_flatMap(vector, f)
    def map[B: Manifest](f: Rep[A] => Rep[B]) = vector_map(vector, f)
    def filter(f: Rep[A] => Rep[Boolean]) = vector_filter(vector, f)
    def save(path: Rep[String]) = vector_save(vector, path)
    def ++(vector2: Rep[Vector[A]]) = vector_++(vector, vector2)
    //    def cache = vector_cache(vector)
  }

  implicit def repVecToVecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, Iterable[V])]]) = new vecIterableTupleOpsCls(x)
  class vecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, Iterable[V])]]) {
    def reduce(f: (Rep[V], Rep[V]) => Rep[V]) = vector_reduce[K, V](x, f)
  }

  implicit def repVecToVecTupleOps[K: Manifest, V: Manifest](x: Rep[Vector[(K, V)]]) = new vecTupleOpsCls(x)
  class vecTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, V)]]) {
    def groupByKey = vector_groupByKey[K, V](x)
  }

  def get_args(): Rep[Array[String]]

  //operations
  def vector_new[A: Manifest](file: Rep[String]): Rep[Vector[String]]
  def vector_map[A: Manifest, B: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[B]): Rep[Vector[B]]
  def vector_flatMap[A: Manifest, B: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[Iterable[B]]): Rep[Vector[B]]
  def vector_filter[A: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[Boolean]): Rep[Vector[A]]
  //  def vector_cache[A: Manifest](vector: Rep[Vector[A]]): Rep[Vector[A]]
  def vector_save[A: Manifest](vector: Rep[Vector[A]], path: Rep[String]): Rep[Unit]
  def vector_++[A: Manifest](vector1: Rep[Vector[A]], vector2: Rep[Vector[A]]): Rep[Vector[A]]
  def vector_reduce[K: Manifest, V: Manifest](vector: Rep[Vector[(K, Iterable[V])]], f: (Rep[V], Rep[V]) => Rep[V]): Rep[Vector[(K, V)]]
  def vector_groupByKey[K: Manifest, V: Manifest](vector: Rep[Vector[(K, V)]]): Rep[Vector[(K, Iterable[V])]]
}

object FakeSourceContext {
  def apply() = SourceContext("unknown", Nil)
}

case class FieldRead(val path: String) {
  val getPath = path.split("\\.").toList
}

trait VectorOpsExp extends VectorOps with VectorBaseExp with FunctionsExp {
  def toAtom2[T: Manifest](d: Def[T])(implicit ctx: SourceContext): Exp[T] = super.toAtom(d)

  trait VectorNode {
    val directFieldReads = mutable.HashSet[FieldRead]()
    val successorFieldReads = mutable.HashSet[FieldRead]()
    def allFieldReads = directFieldReads ++ successorFieldReads
  }

  trait ClosureNode[A, B] extends VectorNode {
    val in: Exp[Vector[_]]
    val func: Exp[A] => Exp[B]
    def getClosureTypes: (Manifest[A], Manifest[B])

    val overrideClosure: Option[Exp[A => B]] = None

    lazy val closure: Exp[A => B] = {
      overrideClosure.getOrElse(
        VectorOpsExp.this.doLambda(func)(getClosureTypes._1, getClosureTypes._2)
      )
    }
  }

  trait Closure2Node[A, B, C] extends VectorNode {
    val in: Exp[Vector[_]]
    val func: (Exp[A], Exp[B]) => Exp[C]
    def getClosureTypes: ((Manifest[A], Manifest[B]), Manifest[C])

    lazy val closure = {
      VectorOpsExp.this.doLambda2(func)(getClosureTypes._1._1, getClosureTypes._1._2, getClosureTypes._2)
    }
  }

  trait ComputationNode extends VectorNode {
    def getTypes: (Manifest[_], Manifest[_])
  }

  trait ComputationNodeTyped[A, B] extends ComputationNode {
    override def getTypes: (Manifest[A], Manifest[B])
  }

  trait PreservingTypeComputation[A] extends ComputationNodeTyped[A, A] {
    def getType: Manifest[A]
    def getTypes = (getType, getType)
  }

  case class NewVector[A: Manifest](file: Exp[String]) extends Def[Vector[String]]
      with ComputationNodeTyped[Nothing, Vector[A]] {
    val mA = manifest[A]
    def getTypes = (manifest[Nothing], manifest[Vector[A]])
  }

  def makeVectorManifest[B: Manifest] = manifest[Vector[B]]

  case class VectorMap[A: Manifest, B: Manifest](in: Exp[Vector[A]], func: Exp[A] => Exp[B])
      extends Def[Vector[B]] with ComputationNodeTyped[Vector[A], Vector[B]] with ClosureNode[A, B] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getClosureTypes = (mA, mB)
    def getTypes = (makeVectorManifest[A], makeVectorManifest[B])
    override def toString = "VectorMap(%s, %s)".format(in, closure)
  }

  case class VectorFilter[A: Manifest](in: Exp[Vector[A]], func: Exp[A] => Exp[Boolean])
      extends Def[Vector[A]] with PreservingTypeComputation[Vector[A]] with ClosureNode[A, Boolean] {
    val mA = manifest[A]
    def getClosureTypes = (mA, Manifest.Boolean)
    def getType = makeVectorManifest[A]
  }

  case class VectorFlatMap[A: Manifest, B: Manifest](in: Exp[Vector[A]], func: Exp[A] => Exp[Iterable[B]])
      extends Def[Vector[B]] with ComputationNodeTyped[Vector[A], Vector[B]] with ClosureNode[A, Iterable[B]] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getTypes = (manifest[Vector[A]], manifest[Vector[B]])
    def getClosureTypes = (manifest[A], manifest[Iterable[B]])
  }

  case class VectorFlatten[A: Manifest](vectors: List[Exp[Vector[A]]]) extends Def[Vector[A]]
      with PreservingTypeComputation[Vector[A]] {
    val mA = manifest[A]
    def getType = manifest[Vector[A]]
  }

  case class VectorGroupByKey[K: Manifest, V: Manifest](v1: Exp[Vector[(K, V)]]) extends Def[Vector[(K, Iterable[V])]]
      with ComputationNodeTyped[Vector[(K, V)], Vector[(K, Iterable[V])]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    val mOutType = manifest[(K, Iterable[V])]
    def getTypes = (manifest[Vector[(K, V)]], manifest[Vector[(K, Iterable[V])]])
  }

  case class VectorReduce[K: Manifest, V: Manifest](in: Exp[Vector[(K, Iterable[V])]], func: (Exp[V], Exp[V]) => Exp[V])
      extends Def[Vector[(K, V)]] with Closure2Node[V, V, V]
      with ComputationNodeTyped[Vector[(K, Iterable[V])], Vector[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getTypes = (manifest[Vector[(K, Iterable[V])]], manifest[Vector[(K, V)]])
  }

  case class VectorSave[A: Manifest](vectors: Exp[Vector[A]], path: Exp[String]) extends Def[Unit]
      with ComputationNodeTyped[Vector[A], Nothing] {
    val mA = manifest[A]
    def getTypes = (manifest[Vector[A]], manifest[Nothing])
  }

  case class GetArgs() extends Def[Array[String]]

  case class Narrowing(struct: Vector[Rep[SimpleStruct[_]]], fields: List[String]) extends Def[Vector[Rep[SimpleStruct[_]]]]

  case class ObjectCreation[A: Manifest](className: String, fields: Map[String, Rep[_]]) extends Def[A] {
    val mA = manifest[A]
  }

  override def get_args() = GetArgs()
  override def vector_new[A: Manifest](file: Exp[String]) = NewVector[A](file)
  override def vector_map[A: Manifest, B: Manifest](vector: Exp[Vector[A]], f: Exp[A] => Exp[B]) = VectorMap[A, B](vector, f)
  override def vector_flatMap[A: Manifest, B: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[Iterable[B]]) = VectorFlatMap(vector, f)
  override def vector_filter[A: Manifest](vector: Rep[Vector[A]], f: Exp[A] => Exp[Boolean]) = VectorFilter(vector, f)
  override def vector_save[A: Manifest](vector: Exp[Vector[A]], file: Exp[String]) = {
    val save = new VectorSave[A](vector, file)
    reflectEffect(save)
  }
  override def vector_++[A: Manifest](vector1: Rep[Vector[A]], vector2: Rep[Vector[A]]) = VectorFlatten(immutable.List(vector1, vector2))
  override def vector_reduce[K: Manifest, V: Manifest](vector: Exp[Vector[(K, Iterable[V])]], f: (Exp[V], Exp[V]) => Exp[V]) = VectorReduce(vector, f)
  override def vector_groupByKey[K: Manifest, V: Manifest](vector: Exp[Vector[(K, V)]]) = VectorGroupByKey(vector)

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = (e match {
    case o @ ObjectCreation(name, fields) => toAtom(ObjectCreation(name, fields.mapValues(f(_)))(o.mA))(o.mA)
    case flat @ VectorFlatten(list) => toAtom(VectorFlatten(f(list))(flat.mA))
    case vm @ NewVector(vector) => toAtom(NewVector(f(vector))(vm.mA))(mtype(vm.mA))
    case vm @ VectorMap(vector, func) => toAtom(
      new { override val overrideClosure = Some(f(vm.closure)) } with VectorMap(f(vector), f(func))(vm.mA, vm.mB)
    )(vm.getTypes._2)
    case vf @ VectorFilter(vector, func) => toAtom(
      new { override val overrideClosure = Some(f(vf.closure)) } with VectorFilter(f(vector), f(func))(vf.mA)
    )(mtype(manifest[A]))
    case vfm @ VectorFlatMap(vector, func) => toAtom(VectorFlatMap(f(vector), f(func))(vfm.mA, vfm.mB))(mtype(manifest[A]))
    case gbk @ VectorGroupByKey(vector) => toAtom(VectorGroupByKey(f(vector))(gbk.mKey, gbk.mValue))(mtype(manifest[A]))
    case v @ VectorReduce(vector, func) => toAtom(VectorReduce(f(vector), f(func))(v.mKey, v.mValue))(mtype(manifest[A]))
    case vs @ VectorSave(vector, path) => toAtom(VectorSave(f(vector), f(path))(vs.mA))
    case Reflect(vs @ VectorSave(vector, path), u, es) => reflectMirrored(Reflect(VectorSave(f(vector), f(path))(vs.mA), mapOver(f, u), f(es)))
    case Reify(x, u, es) => toAtom(Reify(f(x), mapOver(f, u), f(es)))(mtype(manifest[A]))
    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]

  override def syms(e: Any): List[Sym[Any]] = e match {
    case s: ClosureNode[_, _] => syms(s.in, s.closure)
    case s: Closure2Node[_, _, _] => syms(s.in, s.closure)
    case VectorFlatten(x) => syms(x) ++ super.syms(e)
    case NewVector(arg) => syms(arg)
    case VectorSave(vec, path) => syms(vec, path)
    case ObjectCreation(_, fields) => syms(fields)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case s: ClosureNode[_, _] => freqHot(s.closure) ++ freqNormal(s.in)
    case s: Closure2Node[_, _, _] => freqHot(s.closure) ++ freqNormal(s.in)
    case VectorFlatten(x) => freqNormal(x)
    case NewVector(arg) => freqNormal(arg)
    case VectorSave(vec, path) => freqNormal(vec, path)
    case ObjectCreation(_, fields) => freqNormal(fields)
    case _ => super.symsFreq(e)
  }

}

trait VectorImplOps extends VectorOps with FunctionsExp {

}

trait AbstractScalaGenVector extends ScalaGenBase with VectorBaseCodeGenPkg {
  val IR: VectorOpsExp
}

trait ScalaGenVector extends AbstractScalaGenVector with Matchers {
  val IR: VectorOpsExp
  import IR._
  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case nv @ NewVector(filename) => emitValDef(sym, "New vector created from %s with type %s".format(filename, nv.mA))
    case vs @ VectorSave(vector, filename) => stream.println("Saving vector %s (of type %s) to %s".format(vector, vs.mA, filename))
    case vm @ VectorMap(vector, function) => emitValDef(sym, "mapping vector %s with function %s, type %s => %s".format(vector, quote(vm.closure), vm.mA, vm.mB))
    case vf @ VectorFilter(vector, function) => emitValDef(sym, "filtering vector %s with function %s".format(vector, function))
    case vm @ VectorFlatMap(vector, function) => emitValDef(sym, "flat mapping vector %s with function %s".format(vector, function))
    case vm @ VectorFlatten(v1) => emitValDef(sym, "flattening vectors %s".format(v1))
    case gbk @ VectorGroupByKey(vector) => emitValDef(sym, "grouping vector by key")
    case red @ VectorReduce(vector, f) => emitValDef(sym, "reducing vector")
    case GetArgs() => emitValDef(sym, "getting the arguments")
    case _ => super.emitNode(sym, rhs)
  }

  var typeHandler: TypeHandler = null

  def hasVectorNodes(ttps: List[TTP]) = !ttps.flatMap { TTPDef.unapply }.flatMap { case x: VectorNode => Some(x) case _ => None }.isEmpty

  override def fattenAll(e: List[TP[Any]]): List[TTP] = {
    val out = super.fattenAll(e)
    if (!typeHandler.isInstanceOf[TypeHandler] || hasVectorNodes(out)) {
      typeHandler = new TypeHandler(out)
    }
    out
  }

  def writeClosure(closure: Exp[Any => Any]) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    closure match {
      case Def(Lambda(fun, x, y)) => {
        pw.println("{ %s => ".format(quote(x)))
        emitBlock(y)(pw)
        pw.println(quote(getBlockResult(y)))
        pw.print("}")
      }
      case _ =>
    }
    pw.flush
    sw.toString
  }

  def inlineClosures = false

  def handleClosure(closure: Exp[Any => Any]) = {
    if (inlineClosures) {
      writeClosure(closure)
    } else {
      quote(closure)
    }
  }

  class TypeHandler(ttps: List[TTP]) {
    trait PartInfo[A] {
      def m: Manifest[A]
      def niceName: String
    }
    case class FieldInfo[A: Manifest](val name: String, val niceType: String, position: Int) extends PartInfo[A] {
      val m = manifest[A]
      def niceName = niceType
      lazy val containingType = typeInfos2.map(_._2).filter(_.fields.size > position).find(_.fields(position) == this).get
    }
    case class TypeInfo[A: Manifest](val name: String, fields: List[FieldInfo[_]]) extends PartInfo[A] {
      val m = manifest[A]
      def getField(field: String) = fields.find(_.name == field)
      def niceName = name
    }
    val objectCreations = ttps.flatMap {
      case TTP(_, ThinDef(s @ SimpleStruct("tuple2s" :: _, elems))) => None
      case TTP(_, ThinDef(s @ SimpleStruct(tag, elems))) => Some(s)
      case _ => None
    }

    val remappings = objectCreations.map {
      s =>
        (s.m, s.tag.mkString("_"))
    }.toMap
    def cleanUpType(m: Manifest[_]) = {
      var out = m.toString
      remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))
      out
    }
    val typeInfos = objectCreations.map {
      s =>
        (s.tag.mkString("_"), s.elems.mapValues(x => cleanUpType(x.Type)))
    }.toMap
    val typeInfos2 = objectCreations.map {
      s =>
        var i = -1
        val name = s.tag.mkString("_")
        (name, new TypeInfo(name, s.elems.map { x => i += 1; new FieldInfo(x._1, cleanUpType(x._2.Type), i)(x._2.Type) }.toList)(s.m))
    }.toMap

    def getTypeAt(path: String, m: Manifest[Any]): PartInfo[_] = {
      // TODO: This should work for expressions which are nested
      val pathParts = path.split("\\.").drop(1).toList
      var typeNow: Any = m
      pathParts match {
        case Nil =>
        case x :: Nil if remappings.contains(m) => typeNow = typeInfos2(remappings(m)).getField(x).get
        case "_1" :: Nil => typeNow = m.typeArguments(0)
        case "_2" :: Nil => typeNow = m.typeArguments(1)
        case _ => throw new RuntimeException("did not find type for " + path + " in " + m)
      }
      typeNow match {
        case x: TypeInfo[_] => x
        case x: Manifest[_] if remappings.contains(m) => typeInfos2(remappings(m))
        case x: FieldInfo[_] => x
        case _ => throw new RuntimeException("could not use result type " + typeNow + " for " + path + " in " + m)
      }
    }

  }

}
