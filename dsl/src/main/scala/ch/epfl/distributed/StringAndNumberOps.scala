package ch.epfl.distributed

import scala.virtualization.lms.common.PrimitiveOps
import scala.virtualization.lms.common.StringOps
import scala.virtualization.lms.common.StringOpsExp
import scala.virtualization.lms.common.PrimitiveOpsExp
import scala.reflect.SourceContext
import scala.virtualization.lms.internal.ScalaCodegen
import java.io.PrintWriter
import scala.virtualization.lms.util.OverloadHack

trait StringAndNumberOps extends PrimitiveOps with StringOps with OverloadHack {
  def infix_toLong(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Long](s1)
  def infix_toDouble(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Double](s1)
  def infix_toInt(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Int](s1)
  def infix_toByte(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Byte](s1)
  def infix_toFloat(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Float](s1)
  def infix_toBoolean(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Boolean](s1)
  def infix_toShort(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Short](s1)

  def infix_toChar(s1: Rep[String])(implicit ctx: SourceContext) = string_toChar(s1)

  //  def infix_%( l : Rep[Long], mod : Rep[Long])(implicit o: Overloaded2, ctx: SourceContext) = long_modulo(l, mod)
  //  

  def string_toNumber[A <: AnyVal: Manifest](s: Rep[String])(implicit ctx: SourceContext): Rep[A]
  def string_toChar(s: Rep[String])(implicit ctx: SourceContext): Rep[Char]
  //  def long_modulo( l : Rep[Long], mod : Rep[Long])(implicit ctx: SourceContext) : Rep[Long]
  implicit def repStringToStringOps(s: Rep[String]) = new stringOpsCls(s)
  class stringOpsCls(s: Rep[String]) {
    def +(other: Rep[Any]) = string_plus(s, other)
  }

}

trait StringAndNumberOpsExp extends StringAndNumberOps with PrimitiveOpsExp with StringOpsExp {

  case class StringPattern(regex: Exp[String]) extends Def[java.util.regex.Pattern]
  case class StringSplitPattern(s: Exp[String], pattern: Exp[java.util.regex.Pattern], limit: Exp[Int]) extends Def[Array[String]]
  case class StringMatchesPattern(string: Exp[String], pattern: Exp[java.util.regex.Pattern]) extends Def[Boolean]

  case class StringToNumber[A <: AnyVal: Manifest](s: Exp[String]) extends Def[A] {
    val m = manifest[A]
    val typeName = m.toString.reverse.takeWhile(_ != '.').reverse
  }

  case class StringToChar(s: Exp[String]) extends Def[Char]

  //  case class LongModulo(l : Exp[Long], mod : Exp[Long]) extends Def[Long]
  //  
  override def string_toNumber[A <: AnyVal: Manifest](s: Rep[String])(implicit ctx: SourceContext) = StringToNumber[A](s)
  //  override def long_modulo( l : Exp[Long], mod : Exp[Long])(implicit ctx: SourceContext) = LongModulo(l, mod)
  override def string_split(s: Rep[String], separators: Rep[String], limit: Rep[Int]) = StringSplitPattern(s, StringPattern(separators), limit)
  override def string_matches(s: Exp[String], regex: Exp[String]) = StringMatchesPattern(s, StringPattern(regex))

  def string_toChar(s: Rep[String])(implicit ctx: SourceContext) = StringToChar(s)

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = (e match {
    case n @ StringToNumber(s) => string_toNumber(f(s))(n.m, null)
    case n @ StringToChar(s) => string_toChar(f(s))
    case StringPattern(regex) => StringPattern(f(regex))
    case StringSplitPattern(s, pat, l) => StringSplitPattern(f(s), f(pat), f(l))
    case StringMatchesPattern(s, pat) => StringMatchesPattern(f(s), f(pat))
    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]

}

trait StringAndNumberOpsCodeGen extends ScalaCodegen {

  val IR: StringAndNumberOpsExp
  import IR._
  //  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter): Unit = rhs match {
  //    case StringToDouble(s) => emitValDef(sym, "java.lang.Double.valueOf(%s)".format(quote(s)))
  //    case LongModulo(l, mod) => emitValDef(sym, "%s %% %s".format(quote(l), quote(mod)))
  //    case _ => super.emitNode(sym, rhs)
  //  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case StringSplitPattern(s, pattern, limit) => emitValDef(sym, "%s.split(%s, %s)".format(quote(pattern), quote(s), quote(limit)))
    case StringPattern(s) => emitValDef(sym, "java.util.regex.Pattern.compile(%s)".format(quote(s)))
    case StringToChar(s) => emitValDef(sym, "%s.charAt(0)".format(quote(s)))
    case StringMatchesPattern(s, pattern) => emitValDef(sym, "%s.matcher(%s).matches()".format(quote(pattern), quote(s)))
    case n @ StringToNumber(s) => emitValDef(sym, "%s.to%s".format(quote(s), n.typeName))
    case _ => super.emitNode(sym, rhs)(stream)
  }

}
