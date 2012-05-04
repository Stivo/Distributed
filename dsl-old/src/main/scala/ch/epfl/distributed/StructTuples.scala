package ch.epfl.distributed
import scala.virtualization.lms.common.{ TupleOps, TupleOpsExp, BaseExp, StructExp }

trait StructTupleOpsExp extends TupleOpsExp with StructExp {

  override implicit def make_tuple2[A: Manifest, B: Manifest](t: (Exp[A], Exp[B])): Exp[(A, B)] =
    SimpleStruct[Tuple2[A, B]](List("tuple2s", manifest[A].toString, manifest[B].toString), Map("_1" -> t._1, "_2" -> t._2))

  override def tuple2_get1[A: Manifest](t: Exp[(A, _)]) = t match {
    //    case Def(ETuple2(a,b)) => a
    case _ => field[A](t, "_1")
  }
  override def tuple2_get2[B: Manifest](t: Exp[(_, B)]) = t match {
    //    case Def(ETuple2(a,b)) => b
    case _ => field[B](t, "_2")
  }

}