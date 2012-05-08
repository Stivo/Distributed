import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, BaseExp, StructExp, PrimitiveOps }
import scala.virtualization.lms.util.OverloadHack
import scala.util.Random
import scala.virtualization.lms.internal.ScalaCodegen
import scala.virtualization.lms.common.ScalaGenIterableOps

trait VectorBase extends Base with OverloadHack {
  trait Vector

  def Vector(elems: Rep[Array[Double]]): Rep[Vector]

  implicit def vecToVecOps(v: Rep[Vector]) = new vecOps(v)

  class vecOps(v: Rep[Vector]) {
    def *(o: Rep[Vector]) = vec_simpleOp(v, o, "*")
    def +(o: Rep[Vector]) = vec_simpleOp(v, o, "+")
    def -(o: Rep[Vector]) = vec_simpleOp(v, o, "-")
    def /(o: Rep[Vector])(implicit o1: Overloaded1) = vec_simpleOp(v, o, "/")
    def /(o: Rep[Double])(implicit o2: Overloaded2) = vec_pointWiseOp(v, o, "/")
    def squaredDist(o: Rep[Vector]) = vec_squaredDist(v, o)
  }

  def vec_simpleOp(v: Rep[Vector], o: Rep[Vector], op: String): Rep[Vector]
  def vec_pointWiseOp(v: Rep[Vector], o: Rep[Double], op: String): Rep[Vector]
  def vec_squaredDist(v: Rep[Vector], o: Rep[Vector]): Rep[Double]

}

trait VectorBaseExp extends VectorBase with BaseExp {
  case class NewVector(elems: Exp[Array[Double]]) extends Def[Vector]
  case class VectorSimpleOp(v1: Exp[Vector], v2: Exp[Vector], op: String) extends Def[Vector]
  case class VectorPointWiseOp(v: Exp[Vector], d: Exp[Double], op: String) extends Def[Vector]
  case class VectorSquaredDist(v1: Exp[Vector], v2: Exp[Vector]) extends Def[Double]

  def Vector(elems: Exp[Array[Double]]) = NewVector(elems)

  def vec_simpleOp(v: Exp[Vector], o: Exp[Vector], op: String) = VectorSimpleOp(v, o, op)
  def vec_pointWiseOp(v: Exp[Vector], o: Exp[Double], op: String) = VectorPointWiseOp(v, o, op)
  def vec_squaredDist(v: Exp[Vector], o: Exp[Vector]) = VectorSquaredDist(v, o)
}

trait ScalaVectorCodeGen extends ScalaCodegen {
  val IR: VectorBaseExp
  import IR._
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case nv @ NewVector(elems) => emitValDef(sym, quote(elems))
    case VectorSimpleOp(v1, v2, op) => {
      val q1 = quote(v1)
      val q2 = quote(v2)
      emitValDef(sym, """
    	    {
    	    if (%s.size != %s.size)
    			throw new IllegalArgumentException("Should have same length")
    	    val newOut = new Array[Double](%s.size)
    	    var i = 0
    	    while (i < %s.size) {
    			newOut(i) = %s(i) %s %s(i)
    			i += 1
    		}
    	    newOut
    		}
    	    """.format(q1, q2, q1, q1, q1, op, q2))
    }
    case VectorPointWiseOp(v, d, op) => {
      val qv = quote(v)
      val qd = quote(d)
      emitValDef(sym, """
    	    {
    	    val newOut = new Array[Double](%s.size)
    	    var i = 0
    	    while (i < %s.size) {
    			newOut(i) = %s(i) %s %s
    			i += 1
    		}
    	    newOut
    		}
    	    """.format(qv, qv, qv, op, qd))
    }
    case VectorSquaredDist(v1, v2) => {
      val q1 = quote(v1)
      val q2 = quote(v2)
      emitValDef(sym, """
    	    {
    	    if (%s.size != %s.size)
    			throw new IllegalArgumentException("Should have same length")
    	    var out = 0.0
    	    var i = 0
    	    while (i < %s.size) {
    			val dist = %s(i) - %s(i)
    	  		out += dist * dist
    			i += 1
    		}
    	    out
    		}
    	    """.format(q1, q2, q1, q1, q2))
    }
    case _ => super.emitNode(sym, rhs)
  }
  override def remap[A](m: Manifest[A]): String = {
    if (m <:< manifest[Vector]) {
      "Array[Double]"
    } else {
      super.remap(m)
    }
  }
}

trait KMeansApp extends DListProgram with ApplicationOps with SparkDListOps with VectorBase {
  def parseVector(line: Rep[String]): Rep[Vector] = {
    Vector(line.split(" ").map(_.toDouble))
  }

  def closestPoint(p: Rep[Vector], centers: Rep[Array[Vector]]): Rep[Int] = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    var i = 0
    while (i < centers.length) {
      val vCurr = centers(i)
      val tempDist = p.squaredDist(vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      } else { unit(()) }
    }

    return bestIndex
  }

  def statistics(x: Rep[Unit]) = {

    val lines = DList(getArgs(0))
    val data = lines.map(parseVector).cache
    val K = getArgs(1).toInt
    val convergeDist = getArgs(2).toDouble
    // TODO spark code uses take Sample, not available in scoobi
    var points = data.takeSample(false, K, 42).toArray
    var kPoints = NewArray[Vector](K)
    var tempDist = unit(1.0)

    var i = unit(0)
    while (i < points.length - 1) {
      kPoints(i) = points(i)
      i = i + 1
    }

    i = 0
    println(unit("Starting big while"))

    while (tempDist > convergeDist) {
      val closest = data.map { p =>
        val tup: Rep[(Vector, Int)] = (p, unit(1))
        (closestPoint(p, kPoints), tup)
      }
      println(unit("Closest computed"))

      var pointStats = closest.groupByKey.reduce {
        (p1, p2) =>
          (p1._1 + p2._1, p1._2 + p2._2)
      }
      println(unit("pointstats computed"))

      val newPoints = pointStats.map { pair => (pair._1, pair._2._1 / pair._2._2) }.collect()

      println(unit("newPoints computed"))

      tempDist = 0.0
      for (pair <- newPoints) {
        tempDist += kPoints(pair._1).squaredDist(pair._2)
      }

      println(unit("tempDist computed"))
      
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      
      println(unit("kPoints updated"))
      
      println(i)
      println(kPoints)
      println(tempDist)
      println(unit(""))
      i = i + 1

      unit(())
    }

    println(kPoints)
    unit(())
  }

}

class KMeansAppGenerator extends CodeGeneratorTestSuite {

  val appname = "KMeansApp"
  val unoptimizedAppname = appname + "_Orig"

  def testSpark {
    tryCompile {
      println("-- begin")

      val dsl = new KMeansApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp with VectorBaseExp

      val codegen = new { override val allOff = true } with SparkGenDList with ScalaVectorCodeGen with ScalaGenIterableOps { val IR: dsl.type = dsl }
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.statistics, appname, pw)
      println(getContent(pw))
      writeToProject(pw, "spark", appname)
      release(pw)

      println("-- end")
    }
  }

  /*
  def testScoobi {
    tryCompile {
      println("-- begin")

      val dsl = new KMeansApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp

      var pw = setUpPrintWriter
      val codegen = new ScoobiGenDList { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.statistics, appname, pw)
      writeToProject(pw, "scoobi", appname)
      release(pw)

      val typesDefined = codegen.types.keys
      val codegenUnoptimized = new { override val allOff = true } with ScoobiGenDList { val IR: dsl.type = dsl }
      codegenUnoptimized.skipTypes ++= typesDefined
      pw = setUpPrintWriter
      codegenUnoptimized.emitSource(dsl.statistics, unoptimizedAppname, pw)
      writeToProject(pw, "scoobi", unoptimizedAppname)
      release(pw)

      println("-- end")
    }
  }
*/
}
