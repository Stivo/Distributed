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
  //def infix_toLong(s1: Rep[String])(implicit ctx: SourceContext) = string_toLong(s1)
  def infix_toInt(s1: Rep[String])(implicit ctx: SourceContext) = obj_integer_parse_int(s1)
  //  def infix_%( l : Rep[Long], mod : Rep[Long])(implicit o: Overloaded2, ctx: SourceContext) = long_modulo(l, mod)
  //  
  //  def string_toLong(s : Rep[String])(implicit ctx: SourceContext) : Rep[Long]
  //  def long_modulo( l : Rep[Long], mod : Rep[Long])(implicit ctx: SourceContext) : Rep[Long]

  def infix_matches(s: Rep[String], regex: Rep[String]) = string_matches(s, regex)
  def string_matches(s: Rep[String], regex: Rep[String])(implicit ctx: SourceContext): Rep[Boolean]
  //  def string_split(s: Rep[String], separators: Rep[String], limit : Rep[Int])(implicit ctx: SourceContext): Rep[Array[String]]
}

trait StringAndNumberOpsExp extends StringAndNumberOps with PrimitiveOpsExp with StringOpsExp {

  case class StringPattern(regex: Exp[String]) extends Def[java.util.regex.Pattern]
  //     case class StringSplit(s: Exp[String], pattern : Exp[java.util.regex.Pattern], limit : Exp[Int]) extends Def[Array[String]]
  case class StringMatches(string: Exp[String], pattern: Exp[java.util.regex.Pattern]) extends Def[Boolean]

  //  case class StringToLong(s : Exp[String]) extends Def[Long]
  //  case class LongModulo(l : Exp[Long], mod : Exp[Long]) extends Def[Long]
  //  
  //  override def string_toLong(s : Rep[String])(implicit ctx: SourceContext) = StringToLong(s) 
  //  override def long_modulo( l : Exp[Long], mod : Exp[Long])(implicit ctx: SourceContext) = LongModulo(l, mod)
  override def string_matches(s: Exp[String], regex: Exp[String])(implicit ctx: SourceContext) = StringMatches(s, StringPattern(regex))
}

trait StringAndNumberOpsCodeGen extends ScalaCodegen {

  val IR: StringAndNumberOpsExp
  import IR._
  //  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter): Unit = rhs match {
  //    case StringToDouble(s) => emitValDef(sym, "java.lang.Double.valueOf(%s)".format(quote(s)))
  //    case StringToLong(s) => emitValDef(sym, "java.lang.Long.decode(%s)".format(quote(s)))
  //    case LongModulo(l, mod) => emitValDef(sym, "%s %% %s".format(quote(l), quote(mod)))
  //    case _ => super.emitNode(sym, rhs)
  //  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    // case StringSplit(s, pattern, limit) => emitValDef(sym, "%s.split(%s, %s)".format(quote(pattern), quote(s), quote(limit)))
    case StringPattern(s) => emitValDef(sym, "java.util.regex.Pattern.compile(%s)".format(quote(s)))
    case StringMatches(s, pattern) => emitValDef(sym, "%s.matcher(%s).matches()".format(quote(pattern), quote(s)))
    case _ => super.emitNode(sym, rhs)(stream)
  }

}
