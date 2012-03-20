package ch.epfl.distributed

trait Matchers extends ScalaGenVector {
  val IR: VectorOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{ TTP, TP, SubstTransformer, ThinDef, Field }
  import IR.ClosureNode

  object SomeDef {
    def unapply(x: Any): Option[Def[_]] = x match {
      case TTPDef(x) => Some(x)
      case x: Def[_] => Some(x)
      case Def(x) => Some(x)
      //	          case x => Some(x)
      case x => None //{ println("did not match " + x); None }
    }
  }

  object TTPDef {
    def unapply(ttp: TTP) = ttp match {
      case TTP(_, ThinDef(x)) => Some(x)
      case _ => None
    }
  }

  object FieldAccess {
    def unapply(ttp: TTP) = ttp match {
      case TTPDef(f @ Field(obj, field, typ)) => Some(f)
      case _ => None
    }
  }

  object ClosureNode {
    def unapply(any: Any) = any match {
      case TTPDef(cn: ClosureNode[_, _]) => Some(cn)
      case cn: ClosureNode[_, _] => Some(cn)
      case _ => None
    }
  }

  object SomeAccess {
    def unapply(ttp: Any) = ttp match {
      case SomeDef(IR.Tuple2Access1(d)) => Some((d, "._1"))
      case SomeDef(IR.Tuple2Access2(d)) => Some((d, "._2"))
      case SomeDef(IR.Field(struct, name, _)) => Some((struct, "." + name))
      case _ => None
    }
  }
}