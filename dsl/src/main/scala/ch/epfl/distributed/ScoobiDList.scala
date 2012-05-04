package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter

trait ScoobiProgram extends DListProgram

trait ScoobiGenDList extends ScalaGenBase 
    with DListTransformations with Matchers with CaseClassTypeFactory {

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
    DListJoin,
    DListReduce,
    ComputationNode,
    DListNode,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  var wireFormats = List[String]()

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case nv @ NewDList(filename) => emitValDef(sym, "TextInput.fromTextFile(%s)".format(quote(filename)))
      case vs @ DListSave(dlist, filename) => stream.println("DList.persist(TextOutput.toTextFile(%s,%s))".format(quote(dlist), quote(filename)))
      case vm @ DListMap(dlist, function) => emitValDef(sym, "%s.map(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFilter(dlist, function) => emitValDef(sym, "%s.filter(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatten(v1) => {
        var out = "(" + v1.map(quote(_)).mkString(" ++ ")
        out += ")"
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist) => emitValDef(sym, "%s.groupByKey".format(quote(dlist)))
      case v @ DListJoin(left, right) => emitValDef(sym, "join(%s,%s)".format(quote(left), quote(right)))
//      case red @ DListReduce(dlist, f) => emitValDef(sym, "%s.combine(%s)".format(quote(dlist), handleClosure(red.closure)))
      case GetArgs() => emitValDef(sym, "scoobiInputArgs")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val inlineClosures = true
  override val typesInInlinedClosures = true

  val allOff = false
  if (allOff) {
    narrowExistingMaps = false
    insertNarrowingMaps = false
    mapMerge = false
  }

  /*
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
  */
  def mkWireFormats() = {
    val out = new StringBuilder
    def findName(s: String) = s.takeWhile('_' != _)
    val groupedNames = types.keySet.groupBy(findName).map { x => (x._1, (x._2 - (x._1)).toList.sorted) }.toMap
    var index = -1
    val caseClassTypes = groupedNames.values.flatMap(x => x).toList.sorted
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val wireFormat_%s = mkCaseWireFormatGen(%s.apply _, %s.unapply _) "
        .format(index, typ, typ)
    }.mkString("\n")
    out += '\n'
    out ++= groupedNames.map { in =>
      index += 1;
      " implicit val wireFormat_%s = mkAbstractWireFormat%s[%s, %s] "
        .format(index, if (in._2.size == 1) "1" else "", in._1, in._2.mkString(", "))
    }.mkString("\n")
    out ++= "\n//groupings\n"
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val grouping_%s = makeGrouping[%s] "
        .format(index, typ)
    }.mkString("\n")
    out += '\n'
    out.toString
    // emit for each case class: implicit val wireFormat2 = mkCaseWireFormatGen(N2_0_1.apply _, N2_0_1.unapply _)
  }

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, streamIn: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {
    //    val func : Exp[A] => Exp[B] = {x => reifyEffects(f(x))}

    val capture = new StringWriter
    val stream = new PrintWriter(capture)

    stream.println("/*****************************************\n" +
      "  Emitting Scoobi Code                  \n" +
      "*******************************************/")
    stream.println("""
package scoobi.generated;
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat
import ch.epfl.distributed.datastruct._
   
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
    def groupCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
  }
        
  def main(scoobiInputArgsScoobi: Array[String]) = withHadoopArgs(scoobiInputArgsScoobi) { scoobiInputArgs =>
        import WireFormat.{ mkAbstractWireFormat }
        import WireFormatsGen.{ mkCaseWireFormatGen }
        implicit val wireFormat_simpledate = mkCaseWireFormatGen(SimpleDate, SimpleDate.unapply _)
        implicit val wireFormat_datetime = mkCaseWireFormatGen(DateTime, DateTime.unapply _)
   		implicit val wireFormat_date = mkAbstractWireFormat[Date, SimpleDate, DateTime]
    	implicit val grouping_date = makeGrouping[Date]
    	implicit val grouping_simpledate = makeGrouping[SimpleDate]
    	implicit val grouping_datetime = makeGrouping[DateTime]
    	
        ###wireFormats###
        """.format(className, className))

    
    val x = fresh[A]
    val y = reifyBlock(f(x))
    typeHandler = new TypeHandler(y)

    val sA = mA.toString
    val sB = mB.toString

    //    val staticData = getFreeDataBlock(y)

 
    withStream(stream) {
    	emitBlock(y)
    }
    stream.println("}")
    stream.println("}")
    stream.println("// Types that are used in this program")
    stream.println(restTypes.values.toList.sorted.mkString("\n"))

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

trait ScoobiGen extends DListBaseCodeGenPkg with ScoobiGenDList

