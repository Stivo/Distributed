package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import scala.virtualization.lms.common.BooleanOps
import scala.collection.mutable
import scala.virtualization.lms.common.WorklistTransformer
import scala.virtualization.lms.common.ForwardTransformer
import scala.virtualization.lms.internal.Utils

trait DListTransformations extends ScalaGenBase with AbstractScalaGenDList with Matchers with DListAnalysis with Utils {

  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const }
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
    DListNode
  }
  import IR.{ TTP, TP, SubstTransformer }
  import IR.{ findDefinition }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda }
  import IR.{ Struct }

  abstract class TransformationRunner {
    val wt = new WorklistTransformer() { 
      val IR: DListTransformations.this.IR.type = DListTransformations.this.IR
      
      def registerFunction[A](x: Exp[A])(y: () => Exp[A]): Unit = if (nextSubst.contains(x.asInstanceOf[Sym[A]]))
      printdbg("discarding, already have a replacement for " + x)
    else {
      printdbg("register replacement for " + x)
      nextSubst = nextSubst + (x.asInstanceOf[Sym[A]] -> y)
    }
    }
    def run[T: Manifest](y: Block[T]) = {
      registerTransformations(newAnalyzer(y))
      if (wt.nextSubst.isEmpty) {
        y
      } else {
        println("Running")
        wt.run(y)
      }
    }
    def registerTransformations(analyzer: Analyzer)

    def getConsumers(analyzer: Analyzer, x: Sym[_]) = {
      analyzer.statements.filter(_.syms.contains(x))
    }
  }

  class NarrowerInsertionTransformation extends TransformationRunner {
    import wt.IR._

    def makeNarrower[T: Manifest](in: Exp[DList[T]]) = {
      val narrower = dlist_map(wt(in), { x: Rep[T] => x })
      findDefinition(narrower.asInstanceOf[Sym[_]]).get.defs
        .head.asInstanceOf[DListNode].metaInfos("narrower") = true
      narrower
    }
    def registerTransformations(analyzer: Analyzer) {
      analyzer.narrowBefore.foreach {
        case gbk @ DListGroupByKey(x) =>
          val stm = findDefinition(gbk).get
          class GroupByKeyTransformer[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]]) {
            val mapNew = makeNarrower(in)
            val gbkNew = dlist_groupByKey(mapNew)
            wt.register(stm.syms.head)(gbkNew)
          }
          new GroupByKeyTransformer(gbk.dlist)(gbk.mKey, gbk.mValue)

        case j @ DListJoin(l, r) =>
          val stm = findDefinition(j).get
          class DListJoinTransformer[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[DList[(K, V1)]], right: Exp[DList[(K, V2)]]) {
            val mapNewLeft = makeNarrower(left)
            val mapNewRight = makeNarrower(right)

            val joinNew = dlist_join(mapNewLeft, mapNewRight)
            wt.register(stm.syms.head)(joinNew)
          }
          new DListJoinTransformer(l, r)(j.mK, j.mV1, j.mV2)
        case _ =>
      }
    }

  }

  class NarrowMapsTransformation(target: IR.Lambda[_, _], fieldReads: List[FieldRead], typeHandler: TypeHandler) extends TransformationRunner {
    def this(target: DListNode with ClosureNode[_, _], typeHandler: TypeHandler) = this(
      target.closure match {
        case Def(l @ IR.Lambda(_, _, _)) => l
      }, target.successorFieldReads.toList, typeHandler)

    def registerTransformations(analyzer: Analyzer) {
      val targetLambda = analyzer.statements.filter(_.defs.contains(target)).head.syms.head

      val targetSym = target.y.res

      val fields = fieldReads.map(_.path)

      case class Node(val path: String, val children: mutable.Map[String, Node] = mutable.HashMap()) {
        def resolve(pathToChild: String): Option[Node] = {
          val newS = pathToChild.drop(5)
          val arg = if (newS.size >= 1) newS.drop(1) else ""
          resolveInternal(arg)
        }
        private def resolveInternal(pathToChild: String): Option[Node] = {
          if (!pathToChild.isEmpty) {
            val parts = (pathToChild.split("\\.", 2).toList ++ List("")).take(2)
            if (children.contains(parts.head)) {
              children(parts.head).resolveInternal(parts.last)
            } else {
              None
            }
          } else {
            Some(this)
          }
        }
      }
      val out = new Node("input")

      for (x <- fields) {
        var curNode = out
        for (y <- x.split("\\.").drop(1)) {
          if (!curNode.children.contains(y)) {
            val newNode = Node(y)
            curNode.children(y) = newNode
          }
          curNode = curNode.children(y)
        }
      }

      def build[C](path: String, readFromSym: Exp[C]): Exp[C] = {
        import typeHandler.{ TypeInfo, FieldInfo }
        val node = out.resolve(path).get
        val typeInfo = typeHandler.getTypeAt(path, target.y.res.tp)
        printdbg("Typeinfo for path " + path + " is " + typeInfo)
        typeInfo match {
          case ti @ TypeInfo(name, fields) => {
            val elems = for ((childName, node) <- node.children)
              yield (childName, build(path + "." + childName, readFromSym));
            printdbg("Building new Struct with name " + name + " and elems " + elems + " for type " + ti)
            IR.toAtom2(IR.SimpleStruct(IR.ClassTag(name), elems.toMap)(ti.m))(ti.m, FakeSourceContext())
          }
          case fi @ FieldInfo(name, niceType, position) => {
            val newSym = IR.field(readFromSym, name)(fi.m, FakeSourceContext())
            //              val newSym = IR.toAtom2(IR.Field(readFromSym, name, fi.m))(fi.m, FakeSourceContext())
            if (node.children.isEmpty) {
              newSym
            } else {
              val elems = for ((childName, node) <- node.children)
                yield (childName, build(path + "." + childName, newSym));
              val typ = fi.getType
              printdbg("Building new Struct with name " + niceType + " and elems " + elems + " for field " + fi)
              IR.toAtom2(IR.SimpleStruct(IR.ClassTag(niceType), elems.toMap)(typ.m))(typ.m, FakeSourceContext())
            }

          }
        }
      }.asInstanceOf[Exp[C]]
      def h = wt.IR.mtype _
      class LambdaConstructor[A: Manifest, B: Manifest](target: Lambda[A, B]) {
        val newResult = build("input", wt(targetSym))
        val newLam = Lambda(target.f, target.x, wt.IR.Block(wt(newResult)))(wt.IR.mtype(target.mA), wt.IR.mtype(target.mB)).asInstanceOf[Lambda[A, B]]
        val newLamAtom = IR.toAtom2(newLam)(target.m, FakeSourceContext())
      }
      val lc = new LambdaConstructor(target)(wt.IR.mtype(target.mA), wt.IR.mtype(target.mB))
      wt.register(targetLambda)(lc.newLamAtom)
    }
  }

  class MonadicToLoopsTransformation extends TransformationRunner {
    import wt.IR.{ collectYields, fresh, toAtom2,SimpleLoop,reifyEffects, ShapeDep, mtype,
      IteratorCollect, Block, Dummy, IteratorValue, yields, skip, doApply, ifThenElse, reflectMutableSym, 
      reflectMutable, Reflect, Exp }

    def registerTransformations(analyzer: Analyzer) {
      analyzer.nodes.foreach {
        case m @ DListFilter(r, f) =>
          val stm = findDefinition(m).get
          wt.register(stm.syms.head) {
            val i = fresh[Int]
            val d = reflectMutableSym(fresh[Int])
            val value = toAtom2(IteratorValue(wt(r), i))
            val (g, y) = collectYields {
              reifyEffects {
                // Yield the iterator value in the block
                ifThenElse(doApply(wt(f), value), reifyEffects { yields(d, List(i), value)(mtype(r.tp.typeArguments(0))) }, reifyEffects { skip(d, List(i)) })
              }
            }
            // create a loop with the body that inlines the filtering function
            val loop = SimpleLoop(toAtom2(ShapeDep(wt(r))), i, IteratorCollect(g, y))

            // make an stm out of the loop
            toAtom2(loop)(mtype(stm.syms.head.tp), FakeSourceContext())
          }
          System.out.println("Registering " + stm + " to a filter loop")
        case m @ DListMap(r, f) =>
          val stm = findDefinition(m).get
          
          val eval = () => {
          val i = fresh[Int]
   		  val d = reflectMutableSym(fresh[Int])
   		  val closure = Def.unapply(f).get.asInstanceOf[Lambda[Any, Any]]
   		    
          val yld = doApply(wt(f), toAtom2(IteratorValue(wt(r), i)))
   		  val (g, y) = collectYields { reifyEffects {
   		    yields(d, List(i), yld)(closure.mB)
   		  }}
          
          println("Generator type= " + stripGen(g.tp))
          // create a loop with body that inlines the function
          val loop = SimpleLoop(toAtom2(ShapeDep(wt(r))), i, IteratorCollect(g, y))
          
          // make an stm out of the loop
          toAtom2(loop)(mtype(stm.syms.head.tp), FakeSourceContext())
          }
          System.out.println("Registering " + stm + " to a map loop")
          wt.registerFunction(stm.syms.head)(eval)
        case _ =>
      }
    }
  }
  
  /*  

  class MergeFlattenTransformation extends Transformation {
    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(DListFlatten(list)) => {
        list.find { case Def(DListFlatten(list2)) => true case _ => false }.isDefined
      }
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      inExp match {
        case Def(lower @ DListFlatten(list)) =>
          val flat2 = list.find { case Def(DListFlatten(list2)) => true case _ => false }.get
          flat2 match {
            case d @ Def(upper @ DListFlatten(list2)) =>
              val out = new DListFlatten(list.filterNot(_ == d) ++ list2)
              var newDefs = List(out)
              val ttps = newDefs.map(IR.findOrCreateDefinition(_)).map(fatten)
              return (ttps, List((inExp, IR.findOrCreateDefinition(out).sym), (d, IR.findOrCreateDefinition(out).sym)))
          }
      }
      throw new RuntimeException("Bug in merge flatten")
    }

  }

  class SinkFlattenTransformation extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ DListMap(Def(vf @ DListFlatten(list)), func)) => {
        val mappers = list.map { x =>
          val mapper = new DListMap(x, func)
          val newDef = IR.toAtom2(mapper)
          newDef
        }
        new DListFlatten(mappers)
      }
      case _ => null
    }

  }

  class FieldOnStructReadTransformation extends Transformation {

    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => true
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = inExp match {
      case d @ Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => {
        val outDef = elems.get(name).get
        (Nil, List((d, outDef)))
      }
      case _ => throw new RuntimeException("should not be called if appliesToNode returns false")
    }
  }

*/

}
