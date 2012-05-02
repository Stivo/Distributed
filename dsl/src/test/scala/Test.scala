

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
    words1.save("out")
    
//    var i = 0
//    while (i < 5) {
//    i = i + 1
//    }
//    println(i)
     
//	 var words1 = DList("in")
//     (if (getArgs(0).toDouble < 5.0) words1.map(_+"then1").map(_+"then2") else words1).save("out")
     
//     val x = getArgs
//     var y = if (x(0).toDouble < 5.0) x(1) else x(2)
//     println(y)
    
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
  var visited: List[Sym[Any]] = List.empty
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
    visited = List.empty
    transformBlock(s)
  }
  def gatherLiveNodes[A: Manifest](s : Block[A]) = {
    runOnce(s)
    visited
  }
  def run[A:Manifest](s: Block[A]): Block[A] = {
    if (isDone) s else run(runOnce(s))
  }
  override def traverseStm(stm: Stm): Unit = {
    printdbg("Traversing stm "+stm)
    
    stm match {
    
    case TP(sym, rhs) =>
      visited :+= sym
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
      	with BlockExp with Effects with EffectExp with IfThenElse
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
      
      mmt.register(dsl.findDefinition(map2).flatMap {case dsl.TP(x, _) => Some(x) case _ => None}.head,
          new dsl.DListMap(map1.in, map2.func)(map2.mA, map2.mB)
      )

      for (i <- 1 to 2) 
       b1 = mmt.runOnce(b1)
      println("### second")
      println("--- code ---")
      codegen.emitBlock(b1)
//      def visitAll(b : dsl.Block[_]) : List[dsl.Sym[_]] = {
//        def getInputs(x : dsl.Exp[_]) = dsl.syms(x)
//        var out = List[dsl.Sym[_]]()
//        for (input <- getInputs(b.res)) input match {
//          case dsl.Block(x) => out ++= visitAll(x)
//          case dsl.Sym(x) => out :+= x
//          case _ => 
//        }
//        out
//      }
//      mmt.gatherLiveNodes(b1).foreach(println)
      }
      
//      getCurrentDefs(b1)
////      codegen.emitSource(dsl.simple, "g", pw)
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
