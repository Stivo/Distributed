

import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common._
import scala.util.Random
import scala.collection.mutable
import java.io.File
import scala.virtualization.lms.internal._

trait TestProgram extends DListProgram with Config {
   override val verbosity = 2
    def simple(x: Rep[Unit]) = {
    
//    words1
//    .filter(_.matches("\\d+"))
//    .map(_.toInt)
    
    var words1 = DList("in")
    var i = 0
    while (i < 5) {
    words1 = words1.map(_ + "asdf").map(_+"fff")
    i = i + 1
    }
    words1.save(getArgs(1))
    
      
//    DList("in")
//    .map(_+"gfff")
//    .map(_+"fff")
////    .filter(_.length==5)
//    .save("out")
    
    unit(())
  }
  
}

trait MapMapTransformer extends SimpleTransformer {
}


trait SimpleTransformer extends ForwardTransformer { // need backward version, too?
  val IR: LoopsFatExp with IfThenElseFatExp with DListOpsExp
  import IR._
  var curSubst: Map[Sym[Any], Def[Any]] = Map.empty
  var nextSubst: Map[Sym[Any], Def[Any]] = Map.empty
  var innerBlocks: Set[(Manifest[Any], Block[Any])] = Set.empty
  def register[A](x: Exp[A], y : Def[A]): Unit = {
    if (nextSubst.contains(x.asInstanceOf[Sym[A]]))
      printdbg("discarding, already have a replacement for " + x)
    else {
      printdbg("register replacement for " + x)
      nextSubst = nextSubst + (x.asInstanceOf[Sym[A]] -> y)
    }
  }
  def isDone = nextSubst.isEmpty
  def runOnce[A:Manifest](s: Block[A]): Block[A] = {
    subst = Map.empty
    curSubst = nextSubst
    nextSubst = Map.empty
    innerBlocks = Set.empty
    val out = transformBlock(s)
//    innerBlocks.foreach{case (m, b) => transformBlock(b)(m)}
    out
  }
  def run[A:Manifest](s: Block[A]): Block[A] = {
    if (isDone) s else run(runOnce(s))
  }
  override def traverseStm(stm: Stm): Unit = {
    printdbg("Traversing stm "+stm)
    stm match {
      case TP(_, Lambda(_,_,b)) => innerBlocks += ((b.res.tp, b))
      case _ =>
    }
    stm match {
    
    case TP(sym, rhs) => 
      curSubst.get(sym) match {
        case Some(replace) =>
          printdbg("install replacement for " + sym)
          val rep = IR.toAtom2(replace)
          val b = reifyEffects(rep)
          val r = reflectBlock(b)
          subst = subst + (sym -> r)
        case None => 
          super.traverseStm(stm)
      }
  }
  }

}

class TestVectors extends Suite with CodeGenerator {

  def testPrinter {
    try {
      println("-- begin")
//      System.setProperty("lms.verbosity", "2")
      val dsl = new TestProgram with DListProgramExp 
      	with FatExpressions with LoopsFatExp with IfThenElseFatExp
      	with BlockExp with Effects with EffectExp
      val codegen = new PrinterGenerator
      	with SimplifyTransform with GenericFatCodegen with LoopFusionOpt
      	with FatScheduling with BlockTraversal
      	{ val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.withStream(new PrintWriter(System.out)) {
        println("### first")
      var b1 = dsl.reifyEffects(dsl.simple(dsl.fresh))
      val mmt = new MapMapTransformer {val IR: dsl.type = dsl}
      println("--- code ---")
      codegen.emitBlock(b1)
      codegen.stream.flush
      val maps = codegen.availableDefs.flatMap{case dsl.TP(_, x@dsl.DListMap(in, f)) => Some(x) case _ => None}
      val (map1, map2) = (maps(0),maps(1))
      
//      mmt.register(dsl.findDefinition(map2).flatMap {case dsl.TP(x, _) => Some(x) case _ => None}.head,
//          new dsl.DListMap(map1.in, map1.func.asInstanceOf[dsl.Exp[Any] => dsl.Exp[Any]].andThen(map2.func.asInstanceOf[dsl.Exp[Any] => dsl.Exp[Any]]))
//      	)
      
      for (i <- 1 to 2)
       b1 = mmt.runOnce(b1)
      println("### second")
      println("--- code ---")
      codegen.emitBlock(b1)
      }
//      codegen.emitSource(dsl.simple, "g", pw)
      codegen.availableDefs.foreach(println)

//      println(getContent(pw))
//      writeToProject(pw, "spark", "SparkGenerated")
      release(pw)
      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }
  }
}
