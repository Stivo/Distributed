

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
import scala.reflect.SourceContext

trait TestProgram extends DListProgram with Config {
   override val verbosity = 2
    def simple(x: Rep[Unit]) = {
    
//    words1
//    .filter(_.matches("\\d+"))
//    .map(_.toInt)
    


//    var words1 = DList("in")
//    var i = 0
//    while (i < 5) {
//    words1 = words1.map(_ + "asdf").map(_+"fff")
//    i = i + 1
//    }
//    words1.save("out")
    
    
    var i = 0
    while (i < 5) {
      val words1 = DList("in"+i)
      words1.map(_ + "asdf").map(_+"fff").save("out"+i)
      i = i + 1
    }

    
//    var i = 0
//    while (i < 5) {
//    i = i + 1
//    }
//    println(i)
     
//     println(unit("asdf")+unit("fff")+"ffffggh")
     
//	 var words1 = DList("in")
//     (if (getArgs(0).toDouble < 5.0) words1.map(_+"then1").map(_+"then2") else words1).save("out")
     
//     var i = getArgs(1).toDouble
//     println(i)
//     i += getArgs(2).toDouble
//     println(i)
     
//     val x = getArgs
//     var y = if (x(0).toDouble < 5.0) x(1) else x(2)
//     println(y)

//     var x = 0
//     while (x < 10) {
//       x = x + 1
//     }
//     x = 6
//     println(x)
     
//    DList("in")
//    .map(_+"a1")
//    .map(_+"a2")
////    .map(_+"a3")
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
  var manifests: Map[Def[Any], Manifest[_]] = Map.empty
  def register[A:Manifest](x: Exp[A], y : Def[A]): Unit = {
    if (nextSubst.contains(x.asInstanceOf[Sym[A]]))
      printdbg("discarding, already have a replacement for " + x)
    else {
      printdbg("register replacement for " + x)
      nextSubst = nextSubst + (x.asInstanceOf[Sym[A]] -> y)
      manifests += y -> manifest[A]
    }
  }
  def isDone = nextSubst.isEmpty
  def runOnce[A:Manifest](s: Block[A]): Block[A] = {
    subst = Map.empty
    curSubst = nextSubst
    nextSubst = Map.empty
    transformBlock(s)
  }
  def run[A:Manifest](s: Block[A]): Block[A] = {
    if (isDone) s else run(runOnce(s))
  }
  override def traverseStm(stm: Stm): Unit = {
    printdbg("Traversing stm "+stm)
    
    stm match {
    
    case TP(sym, rhs) =>
      curSubst.get(sym) match {
        case Some(replace) =>
          printdbg("install replacement for " + sym)
          val rep = IR.toAtom2(replace)(mtype(manifests(replace)), implicitly[SourceContext])
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
      val dsl = new TestProgram 
        with DListProgramExp 
      	
      val codegen = new PrinterGenerator
      	{ val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.withStream(new PrintWriter(System.out)) {
        println("### first")
      var b1 = dsl.reifyEffects(dsl.simple(dsl.fresh))
      println("--- code ---")
      codegen.emitBlock(b1)
      codegen.stream.flush

//      val mmt = new MapMapTransformer {val IR: dsl.type = dsl}
      val mmt = new WorklistTransformer {val IR: dsl.type = dsl}
//      println(codegen.availableDefs)
       //lambda output replacement code 
      val lambdas = codegen.availableDefs.flatMap{case t@dsl.TP(_,x@ dsl.Lambda(_,_,_)) => Some(x) case _ => Nil}
      println (lambdas)
      
      for (l <- lambdas) {
      val previousOut = l.y.res
      mmt.register(previousOut)(dsl.toAtom2(new dsl.StringPlus(mmt(previousOut), dsl.unit2("transformed"))))
//      mmt.register(previousOut, new dsl.StringPlus(previousOut, dsl.unit2("transformed")))(dsl.mtype(manifest[String]))
      }
      
      
      //replace two mappers with one code
        
//      val maps = codegen.availableDefs.flatMap{case dsl.TP(_, x@dsl.DListMap(in, f)) => Some(x) case _ => None}
//      val (map1, map2) = (maps(0),maps(1))
//      val map2Sym = dsl.infix_lhs(dsl.findDefinition(map2).head).head //.asInstanceOf[dsl.Sym[DList[String]]]
//      println(map2Sym.tp)
//      mmt.register[DList[String]](map2Sym, (dsl.DListMap(map1.in, map2.func)(map2.mA, map2.mB)).asInstanceOf[dsl.Def[DList[String]]])
      
//      mmt.register(map2Sym)(dsl.toAtom2(new dsl.DListMap(mmt(map1.in), mmt(map2.func))(map2.mA, map2.mB)))
//      println(codegen.availableDefs)
//      visitAll(b1.res).foreach(println)
 

        // insert identity mapper
//        val saves = codegen.availableDefs.flatMap{case t@dsl.TP(sym,x@ dsl.Reflect(s@dsl.DListSave(_,_),_,_)) => Some((sym, s)) case _ => Nil}
//        val (sym, save) = saves.head
//        mmt.register(sym)(dsl.toAtom2(new dsl.DListSave(dsl.dlist_map(mmt(save.dlists), { x: dsl.Rep[_] => x }), mmt(save.path))))
        
      for (i <- 1 to 1) 
       b1 = mmt.run(b1)
       
      println("### second")
      println("--- code ---")
      codegen.emitBlock(b1)
      def visitAll(inputSym : dsl.Exp[Any]) : List[dsl.Stm] = {
        def getInputs(x : dsl.Exp[Any]) = x match {
          case x : dsl.Sym[Any] =>
          dsl.findDefinition(x) match {
            case Some(x) => dsl.syms(dsl.infix_rhs(x))
            case None => Nil
          }
          case _ => Nil
        } 
        
        var out = List[dsl.Stm]()
        val inputs = getInputs(inputSym)
        for (input <- inputs) input match {
          case s : dsl.Sym[_] => {
            val stm = dsl.findDefinition(s)
            out ++= (visitAll(s) ++ stm)
          }
          case _ => 
        }
        out.distinct
      }
//      mmt.gatherLiveNodes(b1).foreach(println)
      val nodes = visitAll(b1.res)
//      println("all nodes")
//      nodes.foreach(println)
      /*
       * Idea: 
       * - Analyze tree, prepare a substitution (memorize rule to apply)
       * - Memorize number of symbol that should be targeted
       * - Start transformation
       * - Substitution map allows to find out current symbol for that number
       * - Make callback when the targeted symbol is up
       * 	- Give it the symbol, that was already mirrored, to replace it.
       * 
       */
      //val nodes.flatMap{case dsl.TP(_,dsl.Def(x:dsl.DListNode)) => Some(x) case _ => None}
      //x.foreach(x => println(x+" "+dsl.infix_lhs(x).map(_.tp).mkString(" ")))
      }
    
      
//      getCurrentDefs(b1)
//      codegen.availableDefs.foreach(println)

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

/*
 * Transformers:
 * important:
 * - Change the output of lambda (worked inside while with new transformers, while etc)
 * - Collapse groupbykey reduce into reduceByKey (tried replacing two maps with one: normal: yes, if: no, while: no)
 * - Insert an identity mapper between elements (tried. in outer scope: yes, inside a while: no)
 * (- FieldOnStructRead)
 * - Replace map, filter and flatMap with SimpleLoop
 * 
 */
