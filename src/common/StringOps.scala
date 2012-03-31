package scala.virtualization.lms
package common

import java.io.PrintWriter
import scala.virtualization.lms.util.OverloadHack
import scala.virtualization.lms.internal.{GenerationFailedException}

trait LiftString {
  this: Base =>

  implicit def strToRepStr(s: String) = unit(s)
}

trait StringOps extends Variables with OverloadHack {
  // NOTE: if something doesn't get lifted, this won't give you a compile time error,
  //       since string concat is defined on all objects
  
  def infix_+(s1: Rep[String], s2: Rep[Any]) = string_plus(s1, s2)
  def infix_+(s1: Rep[String], s2: Var[Any])(implicit o: Overloaded1) = string_plus(s1, readVar(s2))
  def infix_+(s1: String, s2: Rep[Any])(implicit o: Overloaded4) = string_plus(unit(s1), s2)
  def infix_+(s1: String, s2: Var[Any])(implicit o: Overloaded5) = string_plus(unit(s1), readVar(s2))
  def infix_+(s1: Rep[Any], s2: Rep[String])(implicit o: Overloaded2) = string_plus(s1, s2)
  def infix_+(s1: Var[Any], s2: Rep[String])(implicit o: Overloaded3) = string_plus(readVar(s1), s2)
  def infix_+(s1: Rep[Any], s2: String)(implicit o: Overloaded6) = string_plus(s1, unit(s2))
  def infix_+(s1: Var[Any], s2: String)(implicit o: Overloaded7) = string_plus(readVar(s1), unit(s2))
  def infix_+(s1: Rep[String], s2: Var[Int])(implicit o: Overloaded8) = string_plus(s1, readVar(s2))
  def infix_+(s1: String, s2: Var[Int])(implicit o: Overloaded9) = string_plus(unit(s1), readVar(s2))

  def infix_trim(s: Rep[String]) = string_trim(s)
  def infix_split(s: Rep[String], separators: Rep[String])(implicit o : Overloaded1) : Rep[Array[String]] = string_split(s, separators, unit(0))
  def infix_split(s: Rep[String], separators: Rep[String], limit : Rep[Int])(implicit o : Overloaded2) : Rep[Array[String]] = string_split(s, separators, limit)
  def infix_startsWith(s1: Rep[String], s2: Rep[String]) = string_startswith(s1,s2)
  def infix_endsWith(s: Rep[String], e: Rep[String]) = string_endsWith(s,e)
  def infix_contains(s1: Rep[String], s2: Rep[String]) = string_contains(s1,s2)
  def infix_isEmpty(s1: Rep[String]) = string_isEmpty(s1)
  def infix_length(s1: Rep[String]) = string_length(s1)
  def infix_matches(s: Rep[String], regex: Rep[String]) = string_matches(s, regex)
  
  object String {
    def valueOf(a: Rep[Any]) = string_valueof(a)
  }

  def string_plus(s: Rep[Any], o: Rep[Any]): Rep[String]
  def string_trim(s: Rep[String]): Rep[String]
  def string_split(s: Rep[String], separators: Rep[String], limit : Rep[Int]): Rep[Array[String]]
  def string_valueof(d: Rep[Any]): Rep[String]
  def string_startswith(s1: Rep[String], s2: Rep[String]): Rep[Boolean]
  def string_endsWith(s: Rep[String], e: Rep[String]): Rep[Boolean]
  def string_contains(s1: Rep[String], s2: Rep[String]) : Rep[Boolean]
  def string_isEmpty(s1: Rep[String]) : Rep[Boolean]
  def string_length(s1: Rep[String]) : Rep[Int]
  def string_matches(s: Rep[String], regex: Rep[String]): Rep[Boolean]
}

trait StringOpsExp extends StringOps with VariablesExp {
  case class StringPlus(s: Exp[Any], o: Exp[Any]) extends Def[String]
  case class StringTrim(s: Exp[String]) extends Def[String]
  case class StringSplit(s: Exp[String], separators: Exp[String], limit : Exp[Int]) extends Def[Array[String]]
  case class StringValueOf(a: Exp[Any]) extends Def[String]
  case class StringStartsWith(s1: Exp[String], s2: Exp[String]) extends Def[Boolean]
  case class StringEndsWith(s: Exp[String], e: Exp[String]) extends Def[Boolean]
  case class StringContains(s: Exp[String], sub: Exp[String]) extends Def[Boolean]
  case class StringIsEmpty(s: Exp[String]) extends Def[Boolean]
  case class StringLength(s: Exp[String]) extends Def[Int]
  case class StringMatches(string: Exp[String], pattern: Exp[String]) extends Def[Boolean]
  
  def string_plus(s: Exp[Any], o: Exp[Any]): Rep[String] = StringPlus(s,o)
  def string_trim(s: Exp[String]) : Rep[String] = StringTrim(s)
  def string_split(s: Exp[String], separators: Exp[String], limit : Exp[Int]) : Rep[Array[String]] = StringSplit(s, separators, limit)
  def string_valueof(a: Exp[Any]) = StringValueOf(a)
  def string_startswith(s1: Exp[String], s2: Exp[String]) = StringStartsWith(s1,s2)
  def string_endsWith(s: Exp[String], e: Exp[String]) = StringEndsWith(s,e)
  def string_contains(s1: Exp[String], s2: Exp[String]) = StringContains(s1,s2)
  def string_isEmpty(s1: Exp[String]) = StringIsEmpty(s1)
  def string_length(s1: Exp[String]) = StringLength(s1)
  def string_matches(s: Rep[String], regex: Rep[String]) = StringMatches(s, regex)
  
  override def mirror[A:Manifest](e: Def[A], f: Transformer): Exp[A] = (e match {
    case StringPlus(a,b) => string_plus(f(a),f(b))
    case StringTrim(s) => string_trim(f(s))
    case StringSplit(s, sep, l) => string_split(f(s), f(sep), f(l))
    case StringValueOf(a) => string_valueof(a)
    case StringStartsWith(s, e) => string_startswith(f(s),f(e))
    case StringEndsWith(s, e) => string_endsWith(f(s),f(e))
    case StringContains(s, sub) => string_contains(f(s),f(sub))
    case StringIsEmpty(s) => string_isEmpty(f(s))
    case StringLength(s) => string_length(f(s))
    case StringMatches(s, pat) => StringMatches(f(s), f(pat))
    case _ => super.mirror(e,f)
  }).asInstanceOf[Exp[A]]
}

trait StringOpsExpOpt extends StringOpsExp {
  override def string_isEmpty(s1: Exp[String]) = s1 match {
    case Const(x : String) => unit(x.isEmpty)
    case _ => super.string_isEmpty(s1)
  }
  override def string_length(s1: Exp[String]) = s1 match {
    case Const(x : String) => unit(x.length)
    case _ => super.string_length(s1)
  }
  override def string_contains(s1: Exp[String], s2 : Exp[String]) = (s1, s2) match {
    case (Const(x : String), Const(y : String)) => unit(x.contains(y))
    case _ => super.string_contains(s1, s2)
  }
  override def string_trim(s1: Exp[String]) = s1 match {
    case Def(s@StringTrim(x)) => s
    case _ => super.string_trim(s1)
  }
}

trait ScalaGenStringOps extends ScalaGenBase {
  val IR: StringOpsExp
  import IR._
  
  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case StringPlus(s1,s2) => emitValDef(sym, "%s+%s".format(quote(s1), quote(s2)))
    case StringTrim(s) => emitValDef(sym, "%s.trim()".format(quote(s)))
    case StringSplit(s, sep, limit) => emitValDef(sym, "%s.split(%s, %s)".format(quote(s), quote(sep), quote(limit)))
    case StringValueOf(a) => emitValDef(sym, "java.lang.String.valueOf(%s)".format(quote(a)))
    case StringStartsWith(s1,s2) => emitValDef(sym, "%s.startsWith(%s)".format(quote(s1),quote(s2)))
    case StringEndsWith(s, e) => emitValDef(sym, "%s.endsWith(%s)".format(quote(s), quote(e)))
    case StringContains(s, e) => emitValDef(sym, "%s.contains(%s)".format(quote(s), quote(e)))
    case StringIsEmpty(s) => emitValDef(sym, "%s.isEmpty()".format(quote(s)))
    case StringLength(s) => emitValDef(sym, "%s.length".format(quote(s)))
    case StringMatches(s, pattern) => emitValDef(sym, "%s.matches(%s)".format(quote(s), quote(pattern)))
    case _ => super.emitNode(sym, rhs)
  }
}

trait CudaGenStringOps extends CudaGenBase {
  val IR: StringOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case StringPlus(s1,s2) => throw new GenerationFailedException("CudaGen: Not GPUable")
    case StringTrim(s) => throw new GenerationFailedException("CudaGen: Not GPUable")
    case StringSplit(s, sep, limit) => throw new GenerationFailedException("CudaGen: Not GPUable")
    case _ => super.emitNode(sym, rhs)
  }
}

trait OpenCLGenStringOps extends OpenCLGenBase {
  val IR: StringOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case StringPlus(s1,s2) => throw new GenerationFailedException("OpenCLGen: Not GPUable")
    case StringTrim(s) => throw new GenerationFailedException("OpenCLGen: Not GPUable")
    case StringSplit(s, sep, limit) => throw new GenerationFailedException("OpenCLGen: Not GPUable")
    case _ => super.emitNode(sym, rhs)
  }
}
trait CGenStringOps extends CGenBase {
  val IR: StringOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case StringPlus(s1,s2) => emitValDef(sym,"strcat(%s,%s);".format(quote(s1),quote(s2)))
    case StringTrim(s) => throw new GenerationFailedException("CGenStringOps: StringTrim not implemented yet")
    case StringSplit(s, sep, limit) => throw new GenerationFailedException("CGenStringOps: StringSplit not implemented yet")
    case _ => super.emitNode(sym, rhs)
  }
}
