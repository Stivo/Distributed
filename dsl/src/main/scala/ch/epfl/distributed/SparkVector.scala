package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter

trait SparkProgram extends VectorOpsExp with VectorImplOps with SparkVectorOpsExp {

}

trait SparkVectorOps extends VectorOps {
  implicit def repVecToSparkVecOps[A: Manifest](vector: Rep[Vector[A]]) = new vecSparkOpsCls(vector)
  class vecSparkOpsCls[A: Manifest](vector: Rep[Vector[A]]) {
    def cache = vector_cache(vector)
  }

  def vector_cache[A: Manifest](vector: Rep[Vector[A]]): Rep[Vector[A]]
}

trait SparkVectorOpsExp extends VectorOpsExp with SparkVectorOps {
  case class VectorReduceByKey[K: Manifest, V: Manifest](in: Exp[Vector[(K, V)]], func: (Exp[V], Exp[V]) => Exp[V])
      extends Def[Vector[(K, V)]] with Closure2Node[V, V, V]
      with PreservingTypeComputation[Vector[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getType = manifest[Vector[(K, V)]]
  }

  case class VectorCache[A: Manifest](in: Exp[Vector[A]]) extends Def[Vector[A]] with PreservingTypeComputation[Vector[A]] {
    val mA = manifest[A]
    def getType = manifest[Vector[A]]
  }

  override def vector_cache[A: Manifest](in: Rep[Vector[A]]) = VectorCache[A](in)

  //  override def vector_map[A: Manifest, B: Manifest](vector: Exp[Vector[A]], f: Exp[A] => Exp[B]) = vector match {
  //    case Def(vm @ VectorMap(in, f2)) => VectorMap(vector, f2.andThen(f))(mtype(vm.mA), manifest[B]) 
  //    case _ => super.vector_map(vector, f)
  //  }

  override def syms(e: Any): List[Sym[Any]] = e match {
    case red: VectorReduceByKey[_, _] => syms(red.in, red.closure)
    case v: VectorCache[_] => syms(v.in)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case red: VectorReduceByKey[_, _] => freqHot(red.closure) ++ freqNormal(red.in)
    case v: VectorCache[_] => freqNormal(v.in)
    case _ => super.symsFreq(e)
  }

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = {
    val out = (e match {
      case v @ VectorReduceByKey(vector, func) => toAtom(
        new { override val overrideClosure = Some(f(v.closure)) } with VectorReduceByKey(f(vector), f(func))(v.mKey, v.mValue)
      )(mtype(manifest[A]))
      case v @ VectorCache(in) => toAtom(VectorCache(f(in))(v.mA))(mtype(v.getType))
      case _ => super.mirror(e, f)
    })
    (e, out) match {
      case (x: VectorNode, Def(y: VectorNode)) => copyMetaInfo(x, y)
      case _ =>
    }
    out.asInstanceOf[Exp[A]]
  }
}

trait SparkTransformations extends VectorTransformations {
  val IR: VectorOpsExp with SparkVectorOpsExp
  import IR.{ VectorReduceByKey, VectorReduce, VectorGroupByKey, VectorMap }
  import IR.{ Def, Exp }

  class ReduceByKeyTransformation extends SimpleSingleConsumerTransformation {
    // TODO: apply if all consumers of group by key are reduces,
    // even if there is more than one
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(red @ VectorReduce(Def(gbk @ VectorGroupByKey(v1)), f1)) => {
        new VectorReduceByKey(v1, f1)(red.mKey, red.mValue)
      }
      case _ => null
    }
  }

  class PullSparkDependenciesTransformation extends PullDependenciesTransformation {

    override def appliesToNodeImpl(inExp: Exp[_], t: Transformer) = {
      inExp match {
        case Def(VectorReduceByKey(in, func)) => true
        case _ => super.appliesToNodeImpl(inExp, t)
      }
    }
    override def doTransformation(inExp: Exp[_]): Def[_] = inExp match {
      case Def(r @ VectorReduceByKey(in, func)) => _doneNodes += inExp; r
      case _ => super.doTransformation(inExp)
    }
  }

}

trait SparkVectorAnalysis extends VectorAnalysis {
  val IR: SparkVectorOpsExp
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
    VectorReduceByKey,
    VectorCache,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, ThinDef, Field }
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  override def newAnalyzer(state: TransformationState, typeHandlerForUse: TypeHandler = typeHandler) = new SparkAnalyzer(state, typeHandlerForUse)

  class SparkAnalyzer(state: TransformationState, typeHandler: TypeHandler) extends Analyzer(state, typeHandler) {

    override def isNarrowBeforeCandidate(x: VectorNode) = x match {
      case VectorReduceByKey(_, _) => true
      case VectorCache(_) => true
      case x => super.isNarrowBeforeCandidate(x)
    }

    override def computeFieldReads(node: VectorNode): Set[FieldRead] = node match {
      case v @ VectorReduceByKey(in, func) => {
        // analyze function
        // convert the analyzed accesses to accesses of input._2
        val part1 = (analyzeFunction(v) ++ Set(FieldRead("input")))
          .map(_.path.drop(5))
          .map(x => "input._2" + x)
          .map(FieldRead)
        // add the accesses from successors
        val part2 = v.successorFieldReads
        val part3 = visitAll("input._1", v.getTypes._1.typeArguments(0))
        (part1 ++ part2 ++ part3).toSet
      }

      case v @ VectorCache(in) => node.successorFieldReads.toSet

      case _ => super.computeFieldReads(node)
    }
  }

}

trait SparkGenVector extends ScalaGenBase with ScalaGenVector with VectorTransformations with SparkTransformations with Matchers with SparkVectorAnalysis {

  val IR: SparkVectorOpsExp
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
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ VectorReduceByKey, VectorCache }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  def makeTypeFor(name: String, fields: Iterable[String]) = {
    val fieldsSet = fields.toSet
    val fieldsInType = typeHandler.typeInfos.getOrElse(name, Map[String, String]())
    val fieldIndexes = fieldsInType.keys.toList
    val indexesHere = fields.map(fieldIndexes.indexOf(_))
    if (!types.contains(name)) {
      types(name) = "trait %s {\n%s\n}".format(name,
        fieldsInType.map {
          case (name, typ) =>
            """def %s : %s = throw new RuntimeException("Should not try to access %s here, internal error")"""
              .format(name, typ, name)
        }.mkString("\n"))
    }
    val typeName = name + ((List("") ++ indexesHere).mkString("_"))
    if (!types.contains(typeName)) {
      val args = fieldsInType.filterKeys(fieldsSet.contains(_)).map { case (name, typ) => "override val %s : %s".format(name, typ) }.mkString(", ")
      types(typeName) = """case class %s(%s) extends %s {
   override def toString() = "%s("+%s+")"
    }""".format(typeName, args, name, name, fieldsInType.keys.map(x => if (fieldsSet.contains(x)) x else "\"\"").mkString("""+","+"""))
    }
    typeName
  }

  val types = mutable.Map[String, String]()

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = {
    val out = rhs match {
      case IR.Field(tuple, x, tp) => emitValDef(sym, "%s.%s".format(quote(tuple), x))
      case IR.SimpleStruct(tag, elems) => emitValDef(sym, "Creating struct with %s and elems %s".format(tag, elems))
      case IR.ObjectCreation(name, fields) if (name.startsWith("tuple2s")) => {
        emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.ObjectCreation(name, fields) => {
        val typeInfo = typeHandler.typeInfos2(name)
        val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
        val typeName = makeTypeFor(name, fieldsList.map(_._1))
        emitValDef(sym, "%s(%s)".format(typeName, fieldsList.map(_._2).map(quote).mkString(", ")))
      }
      case nv @ NewVector(filename) => emitValDef(sym, "sc.textFile(%s)".format(quote(filename)))
      case vs @ VectorSave(vector, filename) => stream.println("%s.saveAsTextFile(%s)".format(quote(vector), quote(filename)))
      case vm @ VectorMap(vector, function) => emitValDef(sym, "%s.map(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFilter(vector, function) => emitValDef(sym, "%s.filter(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFlatMap(vector, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFlatten(v1) => {
        var out = "(" + v1.map(quote(_)).mkString(").union(")
        out += ")"
        emitValDef(sym, out)
      }
      case gbk @ VectorGroupByKey(vector) => emitValDef(sym, "%s.groupByKey".format(quote(vector)))
      case v @ VectorJoin(left, right) => emitValDef(sym, "%s.join(%s)".format(quote(left), quote(right)))
      case red @ VectorReduce(vector, f) => emitValDef(sym, "%s.map(x => (x._1,x._2.reduce(%s)))".format(quote(vector), handleClosure(red.closure)))
      case red @ VectorReduceByKey(vector, f) => emitValDef(sym, "%s.reduceByKey(%s)".format(quote(vector), handleClosure(red.closure)))
      case v @ VectorCache(vector) => emitValDef(sym, "%s.cache()".format(quote(vector)))
      case GetArgs() => emitValDef(sym, "sparkInputArgs.drop(1); // First argument is for spark context")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val inlineClosures = false

  var reduceByKey = true

  var allOff = false
  if (allOff) {
    narrowExistingMaps = false
    insertNarrowingMaps = false
    reduceByKey = false
    mapMerge = false

  }

  override def newPullDeps = new PullSparkDependenciesTransformation

  override def transformTree(state: TransformationState): TransformationState = {
    val transformer = new Transformer(state)
    var pullDeps = newPullDeps
    transformer.doTransformation(pullDeps, 500)

    // perform spark optimizations
    if (mapMerge) {
      //      transformer.currentState.printAll("Before map merge")
      transformer.doTransformation(new MapMergeTransformation, 500)
      transformer.doTransformation(pullDeps, 500)
      //      transformer.currentState.printAll("After map merge")
    }
    if (reduceByKey) {
      transformer.doTransformation(new ReduceByKeyTransformation, 500)
      //pullDeps = new PullSparkDependenciesTransformation()
      transformer.doTransformation(pullDeps, 500)
    }
    mapNarrowingAndInsert(transformer)
    transformer.doTransformation(pullDeps, 500)
    transformer.doTransformation(new TypeTransformations(typeHandler), 500)
    transformer.doTransformation(pullDeps, 500)

    writeGraphToFile(transformer, "test.dot", true)

    transformer.currentState
  }

  override def focusExactScopeFat[A](currentScope0In: List[TTP])(result0B: List[Block[Any]])(body: List[TTP] => A): A = {
    if (hasVectorNodes(currentScope0In)) {
      // set up transformer
      var result0 = result0B.map(getBlockResultFull)
      var state = new TransformationState(currentScope0In, result0)
      val currentScope0 = state.ttps
      result0 = state.results
      // hack: should maybe not add all nodes here, but seems to work, as we are in the top scope
      innerScope ++= IR.globalDefs.filter(!innerScope.contains(_))
      super.focusExactScopeFat(currentScope0)(result0.map(IR.Block(_)))(body)
    } else {
      super.focusExactScopeFat(currentScope0In)(result0B)(body)
    }
  }

  override def remap[A](m: Manifest[A]): String = {
    val remappings = typeHandler.remappings.filter(!_._2.startsWith("tuple2s_"))
    var out = m.toString
    remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))
    out
  }

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    //    val func : Exp[A] => Exp[B] = {x => reifyEffects(f(x))}

    val x = fresh[A]
    val y = reifyBlock(f(x))

    val sA = mA.toString
    val sB = mB.toString

    //    val staticData = getFreeDataBlock(y)

    stream.println("/*****************************************\n" +
      "  Emitting Spark Code                  \n" +
      "*******************************************/")
    stream.println("""
package spark.examples;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo

object %s {
        def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "spark.examples.Registrator")

    		val sc = new SparkContext(sparkInputArgs(0), "%s")
        """.format(className, className))

    emitBlock(y)(stream)

    stream.println("}")
    stream.println("}")
    stream.println("// Types that are used in this program")
    stream.println(types.values.toList.sorted.mkString("\n"))

    stream.println("""class Registrator extends KryoRegistrator {
        def registerClasses(kryo: Kryo) {
        %s
  }
}""".format(types.keys.toList.sorted.map("kryo.register(classOf[" + _ + "])").mkString("\n")))
    stream.println("/*****************************************\n" +
      "  End of Spark Code                  \n" +
      "*******************************************/")

    stream.flush

    //    staticData
    Nil
  }

}

trait SparkGen extends VectorBaseCodeGenPkg with SparkGenVector

