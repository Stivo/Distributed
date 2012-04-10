package ch.epfl.distributed

import scala.virtualization.lms.common.{ ArrayOps, IterableOps, ArrayOpsExp, IterableOpsExp }
import scala.virtualization.lms.internal.ScalaCodegen
import java.io.PrintWriter

trait MoreIterableOps extends ArrayOps with IterableOps {
  implicit def arrayToMoreArrayOps[T: Manifest](a: Array[T]) = new MoreArrayOpsCls(unit(a))
  implicit def repArrayToMoreArrayOps[T: Manifest](a: Rep[Array[T]]) = new MoreArrayOpsCls(a)

  implicit def arrayToIterable[T: Manifest](a: Rep[Array[T]]) = a.asInstanceOf[Rep[Iterable[T]]]

  class MoreArrayOpsCls[T: Manifest](a: Rep[Array[T]]) {

  }

  implicit def arrayToMoreIterableOps[T: Manifest](a: Rep[Array[T]]) = new MoreIterableOpsCls(a)
  implicit def repIterableToMoreIterableOps[T: Manifest](a: Rep[Iterable[T]]) = new MoreIterableOpsCls(a)
  implicit def iterableToMoreIterableOps[T: Manifest](a: Iterable[T]) = new MoreIterableOpsCls(unit(a))

  class MoreIterableOpsCls[T: Manifest](a: Rep[Iterable[T]]) {
    def last = iterable_last(a)
    def first = iterable_first(a)
  }

  def iterable_last[T: Manifest](a: Rep[Iterable[T]]): Rep[T]
  def iterable_first[T: Manifest](a: Rep[Iterable[T]]): Rep[T]

}

trait MoreIterableOpsExp extends MoreIterableOps with ArrayOpsExp with IterableOpsExp {

  case class SingleResultIterableOp[T: Manifest](it: Exp[Iterable[T]], op: String) extends Def[T]

  override def iterable_last[T: Manifest](a: Exp[Iterable[T]]) = SingleResultIterableOp(a, "last")
  override def iterable_first[T: Manifest](a: Exp[Iterable[T]]) = SingleResultIterableOp(a, "first")

  override def mirror[A: Manifest](e: Def[A], f: Transformer): Exp[A] = (e match {
    case SingleResultIterableOp(a, op) => SingleResultIterableOp(f(a), op)
    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]

}

trait MoreIterableOpsCodeGen extends ScalaCodegen {

  val IR: MoreIterableOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter) = rhs match {
    case SingleResultIterableOp(it, s) => emitValDef(sym, "%s.%s".format(quote(it), s))
    case _ => super.emitNode(sym, rhs)(stream)
  }
}
