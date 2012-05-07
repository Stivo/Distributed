package ch.epfl.distributed

import ch.epfl.distributed.datastruct.Date

import java.io.PrintWriter
import scala.virtualization.lms.common.{ ScalaGenBase, ScalaGenEffect, BaseExp, Base }
import scala.reflect.SourceContext

trait DateOps extends Base {

  def infix_toDate(s1: Rep[String])(implicit ctx: SourceContext) = Date(s1)

  //inject interface
  implicit def dateRepToDateRepOps(d: Rep[Date]) = new DateRepOps(d)

  object Date {
    def apply(str: Rep[String]): Rep[Date] = dateObjectApply(str)
  }

  class DateRepOps(d: Rep[Date]) {
    def <=(rd: Rep[Date]): Rep[Boolean] = dateComparison(d, rd, "<=")
    def <(rd: Rep[Date]): Rep[Boolean] = dateComparison(d, rd, "<")
    def +(years: Rep[Int], months: Rep[Int], days: Rep[Int]) = dateAdd(d, years, months, days)
  }

  def dateObjectApply(str: Rep[String]): Rep[Date]
  def dateComparison(ld: Rep[Date], rd: Rep[Date], compare: String): Rep[Boolean]
  def dateAdd(d: Rep[Date], years: Rep[Int], months: Rep[Int], days: Rep[Int]): Rep[Date]

}

trait DateOpsExp extends DateOps with BaseExp {

  //IR nodes
  case class DateObjectApply(str: Rep[String]) extends Def[Date]
  case class DateComparison(ld: Rep[Date], rd: Rep[Date], compare: String) extends Def[Boolean]
  case class DateAdd(d: Rep[Date], years: Rep[Int], months: Rep[Int], days: Rep[Int]) extends Def[Date]

  //Interface implementation
  def dateObjectApply(str: Rep[String]) = DateObjectApply(str)
  def dateComparison(ld: Rep[Date], rd: Rep[Date], compare: String) = DateComparison(ld, rd, compare)
  def dateAdd(d: Rep[Date], years: Rep[Int], months: Rep[Int], days: Rep[Int]) = DateAdd(d, years, months, days)

  override def syms(e: Any): List[Sym[Any]] = e match {
    case DateComparison(l, r, _) => syms(l, r)
    case DateObjectApply(str) => syms(str)
    case DateAdd(d, y, m, days) => syms(d, y, m, days)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case DateComparison(l, r, _) => freqNormal(l, r)
    case DateObjectApply(str) => freqNormal(str)
    case DateAdd(d, y, m, days) => freqNormal(d, y, m, days)
    case _ => super.symsFreq(e)
  }

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = (e match {
    case DateComparison(l, r, c) => dateComparison(f(l), f(r), c)
    case DateObjectApply(s) => dateObjectApply(f(s))
    case DateAdd(d, y, m, days) => dateAdd(f(d), f(y), f(m), f(days))
    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]
}

trait ScalaGenDateOps extends ScalaGenBase {
  val IR: DateOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case DateObjectApply(str: Exp[String]) => emitValDef(sym, "ch.epfl.distributed.datastruct.Date(" + quote(str) + ")")
    case DateComparison(ls, rd, compare) => emitValDef(sym, quote(ls) + " " + compare + " " + quote(rd))
    case DateAdd(d, y, m, days) => emitValDef(sym, "%s + new ch.epfl.distributed.datastruct.Interval(%s, %s, %s)"
      .format(quote(d), quote(y), quote(m), quote(days)))
    case _ => super.emitNode(sym, rhs)
  }

}