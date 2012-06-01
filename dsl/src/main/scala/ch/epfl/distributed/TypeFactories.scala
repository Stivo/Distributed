package ch.epfl.distributed

import scala.collection.mutable

trait TypeFactory extends ScalaGenDList {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  def makeTypeFor(name: String, fields: Iterable[String]): String

  val types = mutable.Map[String, String]()

  val skipTypes = mutable.Set[String]()

  def restTypes = types.filterKeys(x => !skipTypes.contains(x))

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case IR.Field(tuple, x, tp) => emitValDef(sym, "%s.%s".format(quote(tuple), x))
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "new %s(%s)".format(typeName, fieldsList.map(_._2).map(quote).mkString(", ")))
        } catch {
          case e =>
            emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
            e.printStackTrace
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

}

trait CaseClassTypeFactory extends TypeFactory {
  def makeTypeFor(name: String, fields: Iterable[String]): String = {

    // fields is a sorted list of the field names
    // typeInfo is the type with all fields and all infos
    val typeInfo = typeHandler.typeInfos2(name)
    // this is just a set to have contains
    val fieldsSet = fields.toSet
    val fieldsInType = typeInfo.fields
    val fieldsHere = typeInfo.fields.filter(x => fieldsSet.contains(x.name))
    if (!types.contains(name)) {
      types(name) = "trait %s extends Serializable {\n%s\n} ".format(name,
        fieldsInType.map {
          fi =>
            """def %s : %s = throw new RuntimeException("Should not try to access %s here, internal error")"""
              .format(fi.name, fi.niceName, fi.name)
        }.mkString("\n"))
    }
    val typeName = name + ((List("") ++ fieldsHere.map(_.position + "")).mkString("_"))
    if (!types.contains(typeName)) {
      val args = fieldsHere.map { fi => "override val %s : %s".format(fi.name, fi.niceName) }.mkString(", ")
      types(typeName) = """case class %s(%s) extends %s {
   override def toString() = {
        val sb = new StringBuilder()
        sb.append("%s(")
        %s
        sb.append(")")
        sb.toString()
   }
}""".format(typeName, args, name, name,
        fieldsInType
          .map(x => if (fieldsSet.contains(x.name)) x.name else "")
          .map(x => """%s sb.append(",")""".format(if (x.isEmpty) "" else "sb.append(%s); ".format(x)))
          .mkString(";\n"))
    }

    typeName
  }

}

trait WritableTypeFactory extends TypeFactory {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  var useBitset = true
  val bitsetFieldName = "__bitset"
    
  override def makeTypeFor(name: String, fields: Iterable[String]): String = {
	
    // fields is a sorted list of the field names
    // typeInfo is the type with all fields and all infos
    val typeInfo = typeHandler.typeInfos2(name)
    // this is just a set to have contains
    val fieldsSet = fields.toSet
    val fieldsInType = typeInfo.fields
    val fieldsHere = typeInfo.fields.filter(x => fieldsSet.contains(x.name))
    def getDefaultValueForType(x: Manifest[_]) = {
      x match {
        case _ if x <:< manifest[String] => """" """"
        case _ if x <:< manifest[Double] => "0"
        case _ if x <:< manifest[Long] => "0L"
        case _ if x <:< manifest[Int] => "0"
        case _ if x <:< manifest[Char] => "' '"
        case _ if x.toString == "ch.epfl.distributed.datastruct.Date" => "new ch.epfl.distributed.datastruct.Date()"
        case x => throw new RuntimeException("Implement default value for " + x)
      }
    }

    def readAndWriteFields(x: TypeInfo[_]) = {
      def getGuard(x: FieldInfo[_]) = {
        if (useBitset)
          "if ((%s & %s) > 0) ".format(bitsetFieldName, 1 << x.position)
        else
          ""
      }
      def getType(x: FieldInfo[_], read: Boolean) = {
        if (x.m.toString == "ch.epfl.distributed.datastruct.Date") {
          if (read) {
            x.name + ".readFields(in)"
          } else {
            x.name + ".write(out)"
          }
        } else if (x.m <:< manifest[Int]) {
          if (read) {
            x.name + "= WritableUtils.readVInt(in)"
          } else {
            "WritableUtils.writeVInt(out, " + x.name + ")"
          }
        } else if (x.m <:< manifest[Long]) {
          if (read) {
            x.name + "= WritableUtils.readVLong(in)"
          } else {
            "WritableUtils.writeVLong(out, " + x.name + ")"
          }
        } else {
          val t = x.m match {
            case _ if x.m <:< manifest[String] => "UTF"
//            case _ if x.m <:< manifest[Int] => "Int"
            case _ if x.m <:< manifest[Double] => "Double"
            case _ if x.m <:< manifest[Char] => "Char"
            case _ => throw new RuntimeException("Implement read/write functionality for " + x)
          }
          val format = if (read) "%s = in.read%s" else "out.write%2$s(%1$s)"
          format.format(x.name, t)
        }
      }
      val readFieldsBody = x.fields.map { fi =>
        getGuard(fi) + " " + getType(fi, true)
      }.mkString("\n")
      val writeFieldsBody = x.fields.map { fi =>
        getGuard(fi) + " " + getType(fi, false)
      }.mkString("\n")
      val (readBitset, writeBitset) =
        if (useBitset)
          (bitsetFieldName + " = WritableUtils.readVLong(in)", "WritableUtils.writeVLong(out, " + bitsetFieldName + ")")
        else
          ("", "")
      """  override def readFields(in : DataInput) {
      %s
      %s
  }
  override def write(out : DataOutput) {
      %s
      %s
  }""".format(readBitset, readFieldsBody, writeBitset, writeFieldsBody)
    }
    if (!types.contains(name)) {
      val bitSetField = if (useBitset)
        "var %s: Long = %s;".format(bitsetFieldName, (1 << fieldsInType.size) - 1)
      else
        ""
      types(name) = "case class %s (%s) extends Writable {\n%s\n%s\n} ".format(
        name,
        fieldsInType.map {
          fi =>
            """var %s : %s = %s"""
              .format(fi.name, fi.niceName, getDefaultValueForType(fi.m))
        }.mkString(", "),
        bitSetField,
        "def this() = this(%s = %s)\n".format(fieldsInType.head.name, getDefaultValueForType(fieldsInType.head.m))
          + readAndWriteFields(typeInfo))
    }
    name
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) : Unit = {
    val out = rhs match {
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "new %s(%s)".format(typeName, fieldsList.map { case (name, elem) => name + " = " + quote(elem) }.mkString(", ")))
          if (useBitset)
            stream.println(quote(sym)+"."+bitsetFieldName + " = " + fieldsList.map(x => typeInfo.getField(x._1).get.position).map(1 << _).reduce(_ + _))
        } catch {
          case e =>
            emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
            e.printStackTrace
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

}

trait SwitchingTypeFactory extends TypeFactory with CaseClassTypeFactory with WritableTypeFactory {
  val IR: DListOpsExp
  import IR.{ Sym, Def }
  var useWritables = true
  override def getParams() : List[(String, Any)] = {
    super.getParams() ++ List(("using writables", useWritables))
  }
  override def makeTypeFor(name: String, fields: Iterable[String]): String = {
	if (useWritables) {
	  super.makeTypeFor(name, fields)
	} else {
	  super[CaseClassTypeFactory].makeTypeFor(name, fields)
	 }
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) : Unit = {
	if (useWritables) {
	  super.emitNode(sym, rhs)
	} else {
	  super[CaseClassTypeFactory].emitNode(sym, rhs)
	 }
  }
}