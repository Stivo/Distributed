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

trait VectorBase extends Base with LiftAll
  with Equal with IfThenElse with Variables with While with Functions
  with ImplicitOps with NumericOps with OrderingOps with StringOps
  with BooleanOps with PrimitiveOps with MiscOps with TupleOps
  with MathOps with CastingOps with ObjectOps with ArrayOps
  with StringAndNumberOps with ListOps

trait VectorBaseExp extends VectorBase
  with DSLOpsExp with BlockExp
  with EqualExp with IfThenElseExp with VariablesExp with WhileExp with FunctionsExp
  with ImplicitOpsExp with NumericOpsExp with OrderingOpsExp with StringOpsExp
  with BooleanOpsExp with PrimitiveOpsExp with MiscOpsExp with StructTupleOpsExp
  with MathOpsExp with CastingOpsExp with ObjectOpsExp with ArrayOpsExp with RangeOpsExp
  with StructExp with StructExpOpt
  with StructFatExp with StructFatExpOptCommon
  with FatExpressions with LoopsFatExp with IfThenElseFatExp
  with StringAndNumberOpsExp with ListOpsExp

trait VectorBaseCodeGenPkg extends ScalaGenDSLOps
  with SimplifyTransform with ScalaGenIfThenElseFat
  with ScalaGenEqual with ScalaGenIfThenElse with ScalaGenVariables with ScalaGenWhile with ScalaGenFunctions
  with ScalaGenImplicitOps with ScalaGenNumericOps with ScalaGenOrderingOps with ScalaGenStringOps
  with ScalaGenBooleanOps with ScalaGenPrimitiveOps with ScalaGenMiscOps with ScalaGenTupleOps
  with ScalaGenMathOps with ScalaGenCastingOps with ScalaGenObjectOps with ScalaGenArrayOps with ScalaGenRangeOps
  //with ScalaGenFatStruct
  with ScalaGenStruct with GenericFatCodegen
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

  }

  implicit def repVecToVecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, Iterable[V])]]) = new vecIterableTupleOpsCls(x)
  class vecIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, Iterable[V])]]) {
    def reduce(f: (Rep[V], Rep[V]) => Rep[V]) = vector_reduce[K, V](x, f)
  }

  implicit def repVecToVecTupleOps[K: Manifest, V: Manifest](x: Rep[Vector[(K, V)]]) = new vecTupleOpsCls(x)
  class vecTupleOpsCls[K: Manifest, V: Manifest](x: Rep[Vector[(K, V)]]) {
    def groupByKey = vector_groupByKey[K, V](x)
    def join[V2: Manifest](right: Rep[Vector[(K, V2)]]) = vector_join(x, right)
  }

  def get_args(): Rep[Array[String]]

  //operations
  def vector_new[A: Manifest](file: Rep[String]): Rep[Vector[String]]
  def vector_map[A: Manifest, B: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[B]): Rep[Vector[B]]
  def vector_flatMap[A: Manifest, B: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[Iterable[B]]): Rep[Vector[B]]
  def vector_filter[A: Manifest](vector: Rep[Vector[A]], f: Rep[A] => Rep[Boolean]): Rep[Vector[A]]
  def vector_save[A: Manifest](vector: Rep[Vector[A]], path: Rep[String]): Rep[Unit]
  def vector_++[A: Manifest](vector1: Rep[Vector[A]], vector2: Rep[Vector[A]]): Rep[Vector[A]]
  def vector_reduce[K: Manifest, V: Manifest](vector: Rep[Vector[(K, Iterable[V])]], f: (Rep[V], Rep[V]) => Rep[V]): Rep[Vector[(K, V)]]
  def vector_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[Vector[(K, V1)]], right: Rep[Vector[(K, V2)]]): Rep[Vector[(K, (V1, V2))]]
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
    val metaInfos = mutable.Map[String, Any]()
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

    val overrideClosure: Option[Exp[(A, B) => C]] = None

    lazy val closure = {
      overrideClosure.getOrElse(
        VectorOpsExp.this.doLambda2(func)(getClosureTypes._1._1, getClosureTypes._1._2, getClosureTypes._2)
      )
    }
  }

  trait ComputationNode extends VectorNode {
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
    val mInType = manifest[(K, V)]
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

  case class VectorJoin[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[Vector[(K, V1)]], right: Exp[Vector[(K, V2)]])
      extends Def[Vector[(K, (V1, V2))]] with VectorNode {
    def mK = manifest[K]
    def mV1 = manifest[V1]
    def mV2 = manifest[V2]
    def mIn1 = manifest[(K, V1)]
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
  override def vector_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[Vector[(K, V1)]], right: Rep[Vector[(K, V2)]]): Rep[Vector[(K, (V1, V2))]] = VectorJoin(left, right)
  override def vector_groupByKey[K: Manifest, V: Manifest](vector: Exp[Vector[(K, V)]]) = VectorGroupByKey(vector)

  def copyMetaInfo[A <: VectorNode](from: VectorNode, to: A) = { to.metaInfos ++= from.metaInfos; to }

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = {
    var out = e match {
      case o @ ObjectCreation(name, fields) => toAtom(ObjectCreation(name, fields.mapValues(f(_)))(o.mA))(o.mA)
      case flat @ VectorFlatten(list) => toAtom(VectorFlatten(f(list))(flat.mA))
      case vm @ NewVector(vector) => toAtom(NewVector(f(vector))(vm.mA))(mtype(vm.mA))
      case vm @ VectorMap(vector, func) => toAtom(
        new { override val overrideClosure = Some(f(vm.closure)) } with VectorMap(f(vector), f(func))(vm.mA, vm.mB)
      )(vm.getTypes._2)
      case vf @ VectorFilter(vector, func) => toAtom(
        new { override val overrideClosure = Some(f(vf.closure)) } with VectorFilter(f(vector), f(func))(vf.mA)
      )(mtype(manifest[A]))
      case vfm @ VectorFlatMap(vector, func) => toAtom(
        new { override val overrideClosure = Some(f(vfm.closure)) } with VectorFlatMap(f(vector), f(func))(vfm.mA, vfm.mB)
      )(mtype(manifest[A]))
      case gbk @ VectorGroupByKey(vector) => toAtom(VectorGroupByKey(f(vector))(gbk.mKey, gbk.mValue))(mtype(manifest[A]))
      case v @ VectorJoin(left, right) => toAtom(VectorJoin(f(left), f(right))(v.mK, v.mV1, v.mV2))(mtype(manifest[A]))
      case v @ VectorReduce(vector, func) => toAtom(
        new { override val overrideClosure = Some(f(v.closure)) } with VectorReduce(f(vector), f(func))(v.mKey, v.mValue)
      )(mtype(manifest[A]))
      case vs @ VectorSave(vector, path) => toAtom(VectorSave(f(vector), f(path))(vs.mA))
      case Reflect(vs @ VectorSave(vector, path), u, es) => reflectMirrored(Reflect(VectorSave(f(vector), f(path))(vs.mA), mapOver(f, u), f(es)))
      case Reify(x, u, es) => toAtom(Reify(f(x), mapOver(f, u), f(es)))(mtype(manifest[A]))
      case _ => super.mirror(e, f)
    }
    (e, out) match {
      case (x: VectorNode, Def(y: VectorNode)) => copyMetaInfo(x, y)
      case _ =>
    }
    out.asInstanceOf[Exp[A]]
  }
  override def syms(e: Any): List[Sym[Any]] = e match {
    case s: ClosureNode[_, _] => syms(s.in, s.closure)
    case s: Closure2Node[_, _, _] => syms(s.in, s.closure)
    case VectorFlatten(x) => syms(x) ++ super.syms(e)
    case NewVector(arg) => syms(arg)
    case VectorSave(vec, path) => syms(vec, path)
    case ObjectCreation(_, fields) => syms(fields)
    case VectorJoin(left, right) => syms(left, right)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case s: ClosureNode[_, _] => freqHot(s.closure) ++ freqNormal(s.in)
    case s: Closure2Node[_, _, _] => freqHot(s.closure) ++ freqNormal(s.in)
    case VectorFlatten(x) => freqNormal(x)
    case NewVector(arg) => freqNormal(arg)
    case VectorSave(vec, path) => freqNormal(vec, path)
    case ObjectCreation(_, fields) => freqNormal(fields)
    case VectorJoin(left, right) => freqNormal(left, right)
    case _ => super.symsFreq(e)
  }

}

trait VectorImplOps extends VectorOps with FunctionsExp {

}

trait AbstractScalaGenVector extends ScalaGenBase with VectorBaseCodeGenPkg {
  val IR: VectorOpsExp
  import IR.{ TTP, ThinDef, SimpleStruct }

  class TypeHandler(ttps: List[TTP]) {
    trait PartInfo[A] {
      def m: Manifest[A]
      def niceName: String
    }
    case class FieldInfo[A: Manifest](val name: String, val niceType: String, position: Int) extends PartInfo[A] {
      val m = manifest[A]
      def niceName = niceType
      lazy val containingType = typeInfos2.map(_._2).filter(_.fields.size > position).find(_.fields(position) == this).get
      def getType = typeInfos2(niceType)
    }
    case class TypeInfo[A: Manifest](val name: String, val fields: List[FieldInfo[_]]) extends PartInfo[A] {
      val m = manifest[A]
      def getField(field: String) = fields.find(_.name == field)
      def niceName = name
    }
    val objectCreations = ttps.flatMap {
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

    def getTypeAt(path: String, mIn: Manifest[_]): PartInfo[_] = {
      val pathParts = path.split("\\.").drop(1).toList
      val m = mIn.asInstanceOf[Manifest[Any]]
      var typeNow: Any = m
      pathParts match {
        case Nil =>
        case x :: _ if remappings.contains(m) => typeNow = typeInfos2(remappings(m)).getField(x).get
        case "_1" :: _ => typeNow = m.typeArguments(0)
        case "_2" :: _ => typeNow = m.typeArguments(1)
        case _ => throw new RuntimeException("did not find type for " + path + " in " + m)
      }
      val out: PartInfo[_] = typeNow match {
        case x: TypeInfo[_] => x
        case x: Manifest[_] if remappings.contains(m) => typeInfos2(remappings(m))
        case x: FieldInfo[_] => x
        case x: Manifest[(_, _)] => {
          val f1 = new FieldInfo("_1", cleanUpType(x.typeArguments(0)), 0)(x.typeArguments(0))
          val f2 = new FieldInfo("_2", cleanUpType(x.typeArguments(1)), 1)(x.typeArguments(1))
          new TypeInfo("tuple2s", f1 :: f2 :: Nil)
        }
        case _ => throw new RuntimeException("could not use result type " + typeNow + " for " + path + " in " + m)
      }
      def restOfPath(path: List[String], partInfo: PartInfo[_]): PartInfo[_] = {
        path match {
          case Nil => partInfo
          case x :: rest => restOfPath(rest, partInfo.asInstanceOf[FieldInfo[_]].getType.getField(x).get)
        }
      }
      restOfPath(pathParts.drop(1), out)
    }

  }

  var typeHandler: TypeHandler = null

}

trait ScalaGenVector extends AbstractScalaGenVector with Matchers with VectorTransformations with VectorAnalysis {
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
    VectorJoin,
    GetArgs
  }
  import IR.{ SimpleStruct }
  import IR.{ TTP, TP, SubstTransformer, ThinDef, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case nv @ NewVector(filename) => emitValDef(sym, "New vector created from %s with type %s".format(filename, nv.mA))
    case vs @ VectorSave(vector, filename) => stream.println("Saving vector %s (of type %s) to %s".format(vector, vs.mA, filename))
    case vm @ VectorMap(vector, function) => emitValDef(sym, "mapping vector %s with function %s, type %s => %s".format(vector, quote(vm.closure), vm.mA, vm.mB))
    case vf @ VectorFilter(vector, function) => emitValDef(sym, "filtering vector %s with function %s".format(vector, function))
    case vm @ VectorFlatMap(vector, function) => emitValDef(sym, "flat mapping vector %s with function %s".format(vector, function))
    case vm @ VectorFlatten(v1) => emitValDef(sym, "flattening vectors %s".format(v1))
    case gbk @ VectorGroupByKey(vector) => emitValDef(sym, "grouping vector by key")
    case gbk @ VectorJoin(left, right) => emitValDef(sym, "Joining %s with %s".format(left, right))
    case red @ VectorReduce(vector, f) => emitValDef(sym, "reducing vector")
    case GetArgs() => emitValDef(sym, "getting the arguments")
    case IR.Lambda(_, _, _) if inlineClosures =>
    case IR.Lambda2(_, _, _, _) if inlineClosures =>
    case _ => super.emitNode(sym, rhs)
  }

  def hasVectorNodes(ttps: List[TTP]) = !ttps.flatMap { TTPDef.unapply }.flatMap { case x: VectorNode => Some(x) case _ => None }.isEmpty

  override def fattenAll(e: List[TP[Any]]): List[TTP] = {
    val out = super.fattenAll(e)
    if (!typeHandler.isInstanceOf[TypeHandler] || hasVectorNodes(out)) {
      typeHandler = new TypeHandler(out)
    }
    out
  }

  def writeClosure(closure: Exp[_]) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    def remapHere(x: Manifest[_]) = if (typesInInlinedClosures) ": " + remap(x) else ""
    closure match {
      case Def(Lambda(fun, x, y)) => {
        pw.println("{ %s %s => ".format(quote(x), remapHere(x.Type)))
        emitBlock(y)(pw)
        pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.Type)))
        pw.print("}")
      }
      case Def(Lambda2(fun, x1, x2, y)) => {
        pw.println("{ (%s %s, %s %s) => ".format(quote(x1), remapHere(x1.Type), quote(x2), remapHere(x2.Type)))
        emitBlock(y)(pw)
        pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.Type)))
        pw.print("}")
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

  def newPullDeps = new PullDependenciesTransformation()

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
          case v @ VectorMap(in, func) if !v.metaInfos.contains("narrowed")
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
      //inserting narrowing vectormaps where analyzer says it should 
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
    if (hasVectorNodes(currentScope0In)) {
      // set up state
      var result0 = result0B.map(getBlockResultFull)
      var state = new TransformationState(currentScope0In, result0)

      // transform the tree (backend specific)
      state = transformTree(state)

      // return optimized tree to 
      val currentScope0 = state.ttps
      result0 = state.results
      // hack: should maybe not add all nodes here, but seems to work, as we are in the top scope
      innerScope ++= IR.globalDefs.filter(!innerScope.contains(_))
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

}
