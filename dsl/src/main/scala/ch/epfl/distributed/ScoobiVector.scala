package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter

trait ScoobiProgram extends VectorOpsExp with VectorImplOps {

}

trait ScoobiGenVector extends ScalaGenBase with ScalaGenVector with VectorTransformations with Matchers {

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
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
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
  var wireFormats = List[String]()

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
      case nv @ NewVector(filename) => emitValDef(sym, "TextInput.fromTextFile(%s)".format(quote(filename)))
      case vs @ VectorSave(vector, filename) => stream.println("DList.persist(TextOutput.toTextFile(%s,%s))".format(quote(vector), quote(filename)))
      case vm @ VectorMap(vector, function) => emitValDef(sym, "%s.map(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFilter(vector, function) => emitValDef(sym, "%s.filter(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFlatMap(vector, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(vector), handleClosure(vm.closure)))
      case vm @ VectorFlatten(v1) => {
        var out = "(" + v1.map(quote(_)).mkString(" ++ ")
        out += ")"
        emitValDef(sym, out)
      }
      case gbk @ VectorGroupByKey(vector) => emitValDef(sym, "%s.groupByKey".format(quote(vector)))
      case v @ VectorJoin(left, right) => emitValDef(sym, "join(%s,%s)".format(quote(left), quote(right)))
      case red @ VectorReduce(vector, f) => emitValDef(sym, "%s.combine(%s)".format(quote(vector), handleClosure(red.closure)))
      case GetArgs() => emitValDef(sym, "scoobiInputArgs")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val inlineClosures = true
  override val typesInInlinedClosures = true

  var allOff = false
  if (allOff) {
    narrowExistingMaps = false
    insertNarrowingMaps = false
    mapMerge = false
  }

  override def transformTree(state: TransformationState): TransformationState = {
    val transformer = new Transformer(state)
    var pullDeps = newPullDeps
    transformer.doTransformation(pullDeps, 500)

    // perform scoobi optimizations
    if (mapMerge) {
      //      transformer.currentState.printAll("Before map merge")
      transformer.doTransformation(new MapMergeTransformation, 500)
      transformer.doTransformation(pullDeps, 500)
      //      transformer.currentState.printAll("After map merge")
    }
    mapNarrowingAndInsert(transformer)
    transformer.doTransformation(pullDeps, 500)
    transformer.doTransformation(new TypeTransformations(typeHandler), 500)
    transformer.doTransformation(pullDeps, 500)

    writeGraphToFile(transformer, "test.dot", true)

    transformer.currentState
  }

  override def remap[A](m: Manifest[A]): String = {
    val remappings = typeHandler.remappings.filter(!_._2.startsWith("tuple2s_"))
    var out = m.toString
    remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))
    out
  }

  def mkWireFormats() = {
    val out = new StringBuilder
    def findName(s: String) = s.takeWhile('_' != _)
    val groupedNames = types.keySet.groupBy(findName).map { x => (x._1, (x._2 - (x._1)).toList.sorted) }.toMap
    var index = -1
    out ++= groupedNames.map { in =>
      index += 1;
      " implicit val wireFormat_%s = mkAbstractWireFormat%s[%s, %s] "
        .format(index, if (in._2.size == 1) "1" else "", in._1, in._2.mkString(", "))
    }.mkString("\n")
    out += '\n'
    val caseClassTypes = groupedNames.values.flatMap(x => x).toList.sorted
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val wireFormat_%s = mkCaseWireFormat(%s.apply _, %s.unapply _) "
        .format(index, typ, typ)
    }.mkString("\n")
    out ++= "\n//groupings\n"
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val grouping_%s = makeGrouping[%s] "
        .format(index, typ)
    }.mkString("\n")
    out += '\n'
    out.toString
    // emit for each case class: implicit val wireFormat2 = mkCaseWireFormat(N2_0_1.apply _, N2_0_1.unapply _)
  }

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, streamIn: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    //    val func : Exp[A] => Exp[B] = {x => reifyEffects(f(x))}

    val capture = new StringWriter
    val stream = new PrintWriter(capture)

    val x = fresh[A]
    val y = reifyBlock(f(x))

    val sA = mA.toString
    val sB = mB.toString

    //    val staticData = getFreeDataBlock(y)

    stream.println("/*****************************************\n" +
      "  Emitting Scoobi Code                  \n" +
      "*******************************************/")
    stream.println("""
package scoobi.generated;
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import com.nicta.scoobi.{DList, WireFormat, Grouping}
import com.nicta.scoobi.lib.Join._
   
object %s {
   def mkAbstractWireFormat1[T, A <: T: Manifest: WireFormat](): WireFormat[T] = new WireFormat[T] {
    import java.io.{ DataOutput, DataInput }

    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
    }

    override def fromWire(in: DataInput): T =
      implicitly[WireFormat[A]].fromWire(in)
  }
        
  def makeGrouping[A] = new Grouping[A] {
    def sortCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
  }
        
  def main(scoobiInputArgsScoobi: Array[String]) = withHadoopArgs(scoobiInputArgsScoobi) { scoobiInputArgs =>
        import WireFormat.{ mkAbstractWireFormat, mkCaseWireFormat }
        ###wireFormats###
        """.format(className, className))

    emitBlock(y)(stream)

    stream.println("}")
    stream.println("}")
    stream.println("// Types that are used in this program")
    stream.println(types.values.toList.sorted.mkString("\n"))

    stream.println("/*****************************************\n" +
      "  End of Scoobi Code                  \n" +
      "*******************************************/")

    stream.flush
    val out = capture.toString
    val newOut = out.replace("###wireFormats###", mkWireFormats)
    //    staticData
    streamIn.print(newOut)
    Nil
  }

}

trait ScoobiGen extends VectorBaseCodeGenPkg with ScoobiGenVector

