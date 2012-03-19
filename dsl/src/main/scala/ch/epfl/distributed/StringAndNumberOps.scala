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
  //  def infix_toLong(s1: Rep[String])(implicit ctx: SourceContext) = string_toLong(s1)
  //  def infix_toInt(s1: Rep[String])(implicit ctx: SourceContext) = obj_integer_parse_int(s1)
  //  def infix_%( l : Rep[Long], mod : Rep[Long])(implicit o: Overloaded2, ctx: SourceContext) = long_modulo(l, mod)
  //  
  //  def string_toLong(s : Rep[String])(implicit ctx: SourceContext) : Rep[Long]
  //  def long_modulo( l : Rep[Long], mod : Rep[Long])(implicit ctx: SourceContext) : Rep[Long]

}

trait StringAndNumberOpsExp extends StringAndNumberOps with PrimitiveOpsExp with StringOpsExp {

  //  case class StringToLong(s : Exp[String]) extends Def[Long]
  //  case class LongModulo(l : Exp[Long], mod : Exp[Long]) extends Def[Long]
  //  
  //  override def string_toLong(s : Rep[String])(implicit ctx: SourceContext) = StringToLong(s) 
  //  override def long_modulo( l : Exp[Long], mod : Exp[Long])(implicit ctx: SourceContext) = LongModulo(l, mod)
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

}
