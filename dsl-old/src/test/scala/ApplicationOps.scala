/**
 * *********************************************************
 * * AUTOGENERATED USING dsl/lift_user_class.py
 * **********************************************************
 */

import java.io.PrintWriter
import scala.collection.immutable.ListMap
import scala.virtualization.lms.common.ScalaGenFat
import scala.virtualization.lms.util.OverloadHack
import scala.virtualization.lms.common.{ Base, StructExp, EffectExp, BaseFatExp, Variables, StringOps, ArrayOps }
import ch.epfl.distributed.{ StringAndNumberOps, DateOps }
import ch.epfl.distributed.datastruct.Date

trait ParserOps extends StringOps with ArrayOps with StringAndNumberOps with DateOps

trait PartSupplierOps extends Base with Variables with OverloadHack with ParserOps {

  class PartSupplier

  object PartSupplier {
    def apply(ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]) = partsupplier_obj_new(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[PartSupplier] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[PartSupplier] = {
      PartSupplier(input(0).toInt, input(1).toInt, input(2).toInt, input(3).toDouble, input(4))
    }
  }

  implicit def repPartSupplierToPartSupplierOps(x: Rep[PartSupplier]) = new partsupplierOpsCls(x)
  class partsupplierOpsCls(__x: Rep[PartSupplier]) {
    def ps_partkey = partsupplier_ps_partkey(__x)
    def ps_suppkey = partsupplier_ps_suppkey(__x)
    def ps_availqty = partsupplier_ps_availqty(__x)
    def ps_supplycost = partsupplier_ps_supplycost(__x)
    def ps_comment = partsupplier_ps_comment(__x)
  }

  //object defs
  def partsupplier_obj_new(ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]): Rep[PartSupplier]

  //class defs
  def partsupplier_ps_partkey(__x: Rep[PartSupplier]): Rep[Int]
  def partsupplier_ps_suppkey(__x: Rep[PartSupplier]): Rep[Int]
  def partsupplier_ps_availqty(__x: Rep[PartSupplier]): Rep[Int]
  def partsupplier_ps_supplycost(__x: Rep[PartSupplier]): Rep[Double]
  def partsupplier_ps_comment(__x: Rep[PartSupplier]): Rep[String]
}

trait PartSupplierOpsExp extends PartSupplierOps with StructExp with EffectExp with BaseFatExp {
  def partsupplier_obj_new(ps_partkey: Exp[Int], ps_suppkey: Exp[Int], ps_availqty: Exp[Int], ps_supplycost: Exp[Double], ps_comment: Exp[String]) = struct[PartSupplier]("PartSupplier" :: Nil, ListMap("ps_partkey" -> ps_partkey, "ps_suppkey" -> ps_suppkey, "ps_availqty" -> ps_availqty, "ps_supplycost" -> ps_supplycost, "ps_comment" -> ps_comment))
  def partsupplier_ps_partkey(__x: Rep[PartSupplier]) = field[Int](__x, "ps_partkey")
  def partsupplier_ps_suppkey(__x: Rep[PartSupplier]) = field[Int](__x, "ps_suppkey")
  def partsupplier_ps_availqty(__x: Rep[PartSupplier]) = field[Int](__x, "ps_availqty")
  def partsupplier_ps_supplycost(__x: Rep[PartSupplier]) = field[Double](__x, "ps_supplycost")
  def partsupplier_ps_comment(__x: Rep[PartSupplier]) = field[String](__x, "ps_comment")
}

trait LogEntryOps extends Base with Variables with OverloadHack with ParserOps {

  class LogEntry

  object LogEntry {
    def apply(request: Rep[Long], timestamp: Rep[Double], url: Rep[String]) = logentry_obj_new(request, timestamp, url)
    def parse(input: Rep[String], sep: Rep[String]): Rep[LogEntry] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[LogEntry] = {
      LogEntry(input(0).toLong, input(1).toDouble, input(2))
    }
  }

  implicit def repLogEntryToLogEntryOps(x: Rep[LogEntry]) = new logentryOpsCls(x)
  class logentryOpsCls(__x: Rep[LogEntry]) {
    def request = logentry_request(__x)
    def timestamp = logentry_timestamp(__x)
    def url = logentry_url(__x)
  }

  //object defs
  def logentry_obj_new(request: Rep[Long], timestamp: Rep[Double], url: Rep[String]): Rep[LogEntry]

  //class defs
  def logentry_request(__x: Rep[LogEntry]): Rep[Long]
  def logentry_timestamp(__x: Rep[LogEntry]): Rep[Double]
  def logentry_url(__x: Rep[LogEntry]): Rep[String]
}

trait LogEntryOpsExp extends LogEntryOps with StructExp with EffectExp with BaseFatExp {
  def logentry_obj_new(request: Exp[Long], timestamp: Exp[Double], url: Exp[String]) = struct[LogEntry]("LogEntry" :: Nil, ListMap("request" -> request, "timestamp" -> timestamp, "url" -> url))
  def logentry_request(__x: Rep[LogEntry]) = field[Long](__x, "request")
  def logentry_timestamp(__x: Rep[LogEntry]) = field[Double](__x, "timestamp")
  def logentry_url(__x: Rep[LogEntry]) = field[String](__x, "url")
}

trait PageCountEntryOps extends Base with Variables with OverloadHack with ParserOps {

  class PageCountEntry

  object PageCountEntry {
    def apply(language: Rep[String], project: Rep[String], site: Rep[String], number: Rep[Long], size: Rep[Long]) = pagecountentry_obj_new(language, project, site, number, size)
    def parse(input: Rep[String], sep: Rep[String]): Rep[PageCountEntry] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[PageCountEntry] = {
      PageCountEntry(input(0), input(1), input(2), input(3).toLong, input(4).toLong)
    }
  }

  implicit def repPageCountEntryToPageCountEntryOps(x: Rep[PageCountEntry]) = new pagecountentryOpsCls(x)
  class pagecountentryOpsCls(__x: Rep[PageCountEntry]) {
    def language = pagecountentry_language(__x)
    def project = pagecountentry_project(__x)
    def site = pagecountentry_site(__x)
    def number = pagecountentry_number(__x)
    def size = pagecountentry_size(__x)
  }

  //object defs
  def pagecountentry_obj_new(language: Rep[String], project: Rep[String], site: Rep[String], number: Rep[Long], size: Rep[Long]): Rep[PageCountEntry]

  //class defs
  def pagecountentry_language(__x: Rep[PageCountEntry]): Rep[String]
  def pagecountentry_project(__x: Rep[PageCountEntry]): Rep[String]
  def pagecountentry_site(__x: Rep[PageCountEntry]): Rep[String]
  def pagecountentry_number(__x: Rep[PageCountEntry]): Rep[Long]
  def pagecountentry_size(__x: Rep[PageCountEntry]): Rep[Long]
}

trait PageCountEntryOpsExp extends PageCountEntryOps with StructExp with EffectExp with BaseFatExp {
  def pagecountentry_obj_new(language: Exp[String], project: Exp[String], site: Exp[String], number: Exp[Long], size: Exp[Long]) = struct[PageCountEntry]("PageCountEntry" :: Nil, ListMap("language" -> language, "project" -> project, "site" -> site, "number" -> number, "size" -> size))
  def pagecountentry_language(__x: Rep[PageCountEntry]) = field[String](__x, "language")
  def pagecountentry_project(__x: Rep[PageCountEntry]) = field[String](__x, "project")
  def pagecountentry_site(__x: Rep[PageCountEntry]) = field[String](__x, "site")
  def pagecountentry_number(__x: Rep[PageCountEntry]) = field[Long](__x, "number")
  def pagecountentry_size(__x: Rep[PageCountEntry]) = field[Long](__x, "size")
}

trait PartOps extends Base with Variables with OverloadHack with ParserOps {

  class Part

  object Part {
    def apply(p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]) = part_obj_new(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Part] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Part] = {
      Part(input(0).toInt, input(1), input(2), input(3), input(4), input(5).toInt, input(6), input(7).toDouble, input(8))
    }
  }

  implicit def repPartToPartOps(x: Rep[Part]) = new partOpsCls(x)
  class partOpsCls(__x: Rep[Part]) {
    def p_partkey = part_p_partkey(__x)
    def p_name = part_p_name(__x)
    def p_mfgr = part_p_mfgr(__x)
    def p_brand = part_p_brand(__x)
    def p_type = part_p_type(__x)
    def p_size = part_p_size(__x)
    def p_container = part_p_container(__x)
    def p_retailprice = part_p_retailprice(__x)
    def p_comment = part_p_comment(__x)
  }

  //object defs
  def part_obj_new(p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]): Rep[Part]

  //class defs
  def part_p_partkey(__x: Rep[Part]): Rep[Int]
  def part_p_name(__x: Rep[Part]): Rep[String]
  def part_p_mfgr(__x: Rep[Part]): Rep[String]
  def part_p_brand(__x: Rep[Part]): Rep[String]
  def part_p_type(__x: Rep[Part]): Rep[String]
  def part_p_size(__x: Rep[Part]): Rep[Int]
  def part_p_container(__x: Rep[Part]): Rep[String]
  def part_p_retailprice(__x: Rep[Part]): Rep[Double]
  def part_p_comment(__x: Rep[Part]): Rep[String]
}

trait PartOpsExp extends PartOps with StructExp with EffectExp with BaseFatExp {
  def part_obj_new(p_partkey: Exp[Int], p_name: Exp[String], p_mfgr: Exp[String], p_brand: Exp[String], p_type: Exp[String], p_size: Exp[Int], p_container: Exp[String], p_retailprice: Exp[Double], p_comment: Exp[String]) = struct[Part]("Part" :: Nil, ListMap("p_partkey" -> p_partkey, "p_name" -> p_name, "p_mfgr" -> p_mfgr, "p_brand" -> p_brand, "p_type" -> p_type, "p_size" -> p_size, "p_container" -> p_container, "p_retailprice" -> p_retailprice, "p_comment" -> p_comment))
  def part_p_partkey(__x: Rep[Part]) = field[Int](__x, "p_partkey")
  def part_p_name(__x: Rep[Part]) = field[String](__x, "p_name")
  def part_p_mfgr(__x: Rep[Part]) = field[String](__x, "p_mfgr")
  def part_p_brand(__x: Rep[Part]) = field[String](__x, "p_brand")
  def part_p_type(__x: Rep[Part]) = field[String](__x, "p_type")
  def part_p_size(__x: Rep[Part]) = field[Int](__x, "p_size")
  def part_p_container(__x: Rep[Part]) = field[String](__x, "p_container")
  def part_p_retailprice(__x: Rep[Part]) = field[Double](__x, "p_retailprice")
  def part_p_comment(__x: Rep[Part]) = field[String](__x, "p_comment")
}

trait N1Ops extends Base with Variables with OverloadHack with ParserOps with N2Ops {

  class N1

  object N1 {
    def apply(n2: Rep[N2], n1id: Rep[String], n1Junk: Rep[Int]) = n1_obj_new(n2, n1id, n1Junk)
  }

  implicit def repN1ToN1Ops(x: Rep[N1]) = new n1OpsCls(x)
  class n1OpsCls(__x: Rep[N1]) {
    def n2 = n1_n2(__x)
    def n1id = n1_n1id(__x)
    def n1Junk = n1_n1Junk(__x)
  }

  //object defs
  def n1_obj_new(n2: Rep[N2], n1id: Rep[String], n1Junk: Rep[Int]): Rep[N1]

  //class defs
  def n1_n2(__x: Rep[N1]): Rep[N2]
  def n1_n1id(__x: Rep[N1]): Rep[String]
  def n1_n1Junk(__x: Rep[N1]): Rep[Int]
}

trait N1OpsExp extends N1Ops with StructExp with EffectExp with BaseFatExp {
  def n1_obj_new(n2: Exp[N2], n1id: Exp[String], n1Junk: Exp[Int]) = struct[N1]("N1" :: Nil, ListMap("n2" -> n2, "n1id" -> n1id, "n1Junk" -> n1Junk))
  def n1_n2(__x: Rep[N1]) = field[N2](__x, "n2")
  def n1_n1id(__x: Rep[N1]) = field[String](__x, "n1id")
  def n1_n1Junk(__x: Rep[N1]) = field[Int](__x, "n1Junk")
}

trait RegionOps extends Base with Variables with OverloadHack with ParserOps {

  class Region

  object Region {
    def apply(r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]) = region_obj_new(r_regionkey, r_name, r_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Region] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Region] = {
      Region(input(0).toInt, input(1), input(2))
    }
  }

  implicit def repRegionToRegionOps(x: Rep[Region]) = new regionOpsCls(x)
  class regionOpsCls(__x: Rep[Region]) {
    def r_regionkey = region_r_regionkey(__x)
    def r_name = region_r_name(__x)
    def r_comment = region_r_comment(__x)
  }

  //object defs
  def region_obj_new(r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]): Rep[Region]

  //class defs
  def region_r_regionkey(__x: Rep[Region]): Rep[Int]
  def region_r_name(__x: Rep[Region]): Rep[String]
  def region_r_comment(__x: Rep[Region]): Rep[String]
}

trait RegionOpsExp extends RegionOps with StructExp with EffectExp with BaseFatExp {
  def region_obj_new(r_regionkey: Exp[Int], r_name: Exp[String], r_comment: Exp[String]) = struct[Region]("Region" :: Nil, ListMap("r_regionkey" -> r_regionkey, "r_name" -> r_name, "r_comment" -> r_comment))
  def region_r_regionkey(__x: Rep[Region]) = field[Int](__x, "r_regionkey")
  def region_r_name(__x: Rep[Region]) = field[String](__x, "r_name")
  def region_r_comment(__x: Rep[Region]) = field[String](__x, "r_comment")
}

trait CustomerOps extends Base with Variables with OverloadHack with ParserOps {

  class Customer

  object Customer {
    def apply(c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]) = customer_obj_new(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Customer] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Customer] = {
      Customer(input(0).toInt, input(1), input(2), input(3).toInt, input(4), input(5).toDouble, input(6), input(7))
    }
  }

  implicit def repCustomerToCustomerOps(x: Rep[Customer]) = new customerOpsCls(x)
  class customerOpsCls(__x: Rep[Customer]) {
    def c_custkey = customer_c_custkey(__x)
    def c_name = customer_c_name(__x)
    def c_address = customer_c_address(__x)
    def c_nationkey = customer_c_nationkey(__x)
    def c_phone = customer_c_phone(__x)
    def c_acctbal = customer_c_acctbal(__x)
    def c_mktsegment = customer_c_mktsegment(__x)
    def c_comment = customer_c_comment(__x)
  }

  //object defs
  def customer_obj_new(c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]): Rep[Customer]

  //class defs
  def customer_c_custkey(__x: Rep[Customer]): Rep[Int]
  def customer_c_name(__x: Rep[Customer]): Rep[String]
  def customer_c_address(__x: Rep[Customer]): Rep[String]
  def customer_c_nationkey(__x: Rep[Customer]): Rep[Int]
  def customer_c_phone(__x: Rep[Customer]): Rep[String]
  def customer_c_acctbal(__x: Rep[Customer]): Rep[Double]
  def customer_c_mktsegment(__x: Rep[Customer]): Rep[String]
  def customer_c_comment(__x: Rep[Customer]): Rep[String]
}

trait CustomerOpsExp extends CustomerOps with StructExp with EffectExp with BaseFatExp {
  def customer_obj_new(c_custkey: Exp[Int], c_name: Exp[String], c_address: Exp[String], c_nationkey: Exp[Int], c_phone: Exp[String], c_acctbal: Exp[Double], c_mktsegment: Exp[String], c_comment: Exp[String]) = struct[Customer]("Customer" :: Nil, ListMap("c_custkey" -> c_custkey, "c_name" -> c_name, "c_address" -> c_address, "c_nationkey" -> c_nationkey, "c_phone" -> c_phone, "c_acctbal" -> c_acctbal, "c_mktsegment" -> c_mktsegment, "c_comment" -> c_comment))
  def customer_c_custkey(__x: Rep[Customer]) = field[Int](__x, "c_custkey")
  def customer_c_name(__x: Rep[Customer]) = field[String](__x, "c_name")
  def customer_c_address(__x: Rep[Customer]) = field[String](__x, "c_address")
  def customer_c_nationkey(__x: Rep[Customer]) = field[Int](__x, "c_nationkey")
  def customer_c_phone(__x: Rep[Customer]) = field[String](__x, "c_phone")
  def customer_c_acctbal(__x: Rep[Customer]) = field[Double](__x, "c_acctbal")
  def customer_c_mktsegment(__x: Rep[Customer]) = field[String](__x, "c_mktsegment")
  def customer_c_comment(__x: Rep[Customer]) = field[String](__x, "c_comment")
}

trait OrderOps extends Base with Variables with OverloadHack with ParserOps {

  class Order

  object Order {
    def apply(o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Date], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]) = order_obj_new(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Order] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Order] = {
      Order(input(0).toInt, input(1).toInt, input(2).toChar, input(3).toDouble, input(4).toDate, input(5), input(6), input(7).toInt, input(8))
    }
  }

  implicit def repOrderToOrderOps(x: Rep[Order]) = new orderOpsCls(x)
  class orderOpsCls(__x: Rep[Order]) {
    def o_orderkey = order_o_orderkey(__x)
    def o_custkey = order_o_custkey(__x)
    def o_orderstatus = order_o_orderstatus(__x)
    def o_totalprice = order_o_totalprice(__x)
    def o_orderdate = order_o_orderdate(__x)
    def o_orderpriority = order_o_orderpriority(__x)
    def o_clerk = order_o_clerk(__x)
    def o_shippriority = order_o_shippriority(__x)
    def o_comment = order_o_comment(__x)
  }

  //object defs
  def order_obj_new(o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Date], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]): Rep[Order]

  //class defs
  def order_o_orderkey(__x: Rep[Order]): Rep[Int]
  def order_o_custkey(__x: Rep[Order]): Rep[Int]
  def order_o_orderstatus(__x: Rep[Order]): Rep[Char]
  def order_o_totalprice(__x: Rep[Order]): Rep[Double]
  def order_o_orderdate(__x: Rep[Order]): Rep[Date]
  def order_o_orderpriority(__x: Rep[Order]): Rep[String]
  def order_o_clerk(__x: Rep[Order]): Rep[String]
  def order_o_shippriority(__x: Rep[Order]): Rep[Int]
  def order_o_comment(__x: Rep[Order]): Rep[String]
}

trait OrderOpsExp extends OrderOps with StructExp with EffectExp with BaseFatExp {
  def order_obj_new(o_orderkey: Exp[Int], o_custkey: Exp[Int], o_orderstatus: Exp[Char], o_totalprice: Exp[Double], o_orderdate: Exp[Date], o_orderpriority: Exp[String], o_clerk: Exp[String], o_shippriority: Exp[Int], o_comment: Exp[String]) = struct[Order]("Order" :: Nil, ListMap("o_orderkey" -> o_orderkey, "o_custkey" -> o_custkey, "o_orderstatus" -> o_orderstatus, "o_totalprice" -> o_totalprice, "o_orderdate" -> o_orderdate, "o_orderpriority" -> o_orderpriority, "o_clerk" -> o_clerk, "o_shippriority" -> o_shippriority, "o_comment" -> o_comment))
  def order_o_orderkey(__x: Rep[Order]) = field[Int](__x, "o_orderkey")
  def order_o_custkey(__x: Rep[Order]) = field[Int](__x, "o_custkey")
  def order_o_orderstatus(__x: Rep[Order]) = field[Char](__x, "o_orderstatus")
  def order_o_totalprice(__x: Rep[Order]) = field[Double](__x, "o_totalprice")
  def order_o_orderdate(__x: Rep[Order]) = field[Date](__x, "o_orderdate")
  def order_o_orderpriority(__x: Rep[Order]) = field[String](__x, "o_orderpriority")
  def order_o_clerk(__x: Rep[Order]) = field[String](__x, "o_clerk")
  def order_o_shippriority(__x: Rep[Order]) = field[Int](__x, "o_shippriority")
  def order_o_comment(__x: Rep[Order]) = field[String](__x, "o_comment")
}

trait SupplierOps extends Base with Variables with OverloadHack with ParserOps {

  class Supplier

  object Supplier {
    def apply(s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]) = supplier_obj_new(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Supplier] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Supplier] = {
      Supplier(input(0).toInt, input(1), input(2), input(3).toInt, input(4), input(5).toDouble, input(6))
    }
  }

  implicit def repSupplierToSupplierOps(x: Rep[Supplier]) = new supplierOpsCls(x)
  class supplierOpsCls(__x: Rep[Supplier]) {
    def s_suppkey = supplier_s_suppkey(__x)
    def s_name = supplier_s_name(__x)
    def s_address = supplier_s_address(__x)
    def s_nationkey = supplier_s_nationkey(__x)
    def s_phone = supplier_s_phone(__x)
    def s_acctbal = supplier_s_acctbal(__x)
    def s_comment = supplier_s_comment(__x)
  }

  //object defs
  def supplier_obj_new(s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]): Rep[Supplier]

  //class defs
  def supplier_s_suppkey(__x: Rep[Supplier]): Rep[Int]
  def supplier_s_name(__x: Rep[Supplier]): Rep[String]
  def supplier_s_address(__x: Rep[Supplier]): Rep[String]
  def supplier_s_nationkey(__x: Rep[Supplier]): Rep[Int]
  def supplier_s_phone(__x: Rep[Supplier]): Rep[String]
  def supplier_s_acctbal(__x: Rep[Supplier]): Rep[Double]
  def supplier_s_comment(__x: Rep[Supplier]): Rep[String]
}

trait SupplierOpsExp extends SupplierOps with StructExp with EffectExp with BaseFatExp {
  def supplier_obj_new(s_suppkey: Exp[Int], s_name: Exp[String], s_address: Exp[String], s_nationkey: Exp[Int], s_phone: Exp[String], s_acctbal: Exp[Double], s_comment: Exp[String]) = struct[Supplier]("Supplier" :: Nil, ListMap("s_suppkey" -> s_suppkey, "s_name" -> s_name, "s_address" -> s_address, "s_nationkey" -> s_nationkey, "s_phone" -> s_phone, "s_acctbal" -> s_acctbal, "s_comment" -> s_comment))
  def supplier_s_suppkey(__x: Rep[Supplier]) = field[Int](__x, "s_suppkey")
  def supplier_s_name(__x: Rep[Supplier]) = field[String](__x, "s_name")
  def supplier_s_address(__x: Rep[Supplier]) = field[String](__x, "s_address")
  def supplier_s_nationkey(__x: Rep[Supplier]) = field[Int](__x, "s_nationkey")
  def supplier_s_phone(__x: Rep[Supplier]) = field[String](__x, "s_phone")
  def supplier_s_acctbal(__x: Rep[Supplier]) = field[Double](__x, "s_acctbal")
  def supplier_s_comment(__x: Rep[Supplier]) = field[String](__x, "s_comment")
}

trait NationOps extends Base with Variables with OverloadHack with ParserOps {

  class Nation

  object Nation {
    def apply(n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]) = nation_obj_new(n_nationkey, n_name, n_regionkey, n_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Nation] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Nation] = {
      Nation(input(0).toInt, input(1), input(2).toInt, input(3))
    }
  }

  implicit def repNationToNationOps(x: Rep[Nation]) = new nationOpsCls(x)
  class nationOpsCls(__x: Rep[Nation]) {
    def n_nationkey = nation_n_nationkey(__x)
    def n_name = nation_n_name(__x)
    def n_regionkey = nation_n_regionkey(__x)
    def n_comment = nation_n_comment(__x)
  }

  //object defs
  def nation_obj_new(n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]): Rep[Nation]

  //class defs
  def nation_n_nationkey(__x: Rep[Nation]): Rep[Int]
  def nation_n_name(__x: Rep[Nation]): Rep[String]
  def nation_n_regionkey(__x: Rep[Nation]): Rep[Int]
  def nation_n_comment(__x: Rep[Nation]): Rep[String]
}

trait NationOpsExp extends NationOps with StructExp with EffectExp with BaseFatExp {
  def nation_obj_new(n_nationkey: Exp[Int], n_name: Exp[String], n_regionkey: Exp[Int], n_comment: Exp[String]) = struct[Nation]("Nation" :: Nil, ListMap("n_nationkey" -> n_nationkey, "n_name" -> n_name, "n_regionkey" -> n_regionkey, "n_comment" -> n_comment))
  def nation_n_nationkey(__x: Rep[Nation]) = field[Int](__x, "n_nationkey")
  def nation_n_name(__x: Rep[Nation]) = field[String](__x, "n_name")
  def nation_n_regionkey(__x: Rep[Nation]) = field[Int](__x, "n_regionkey")
  def nation_n_comment(__x: Rep[Nation]) = field[String](__x, "n_comment")
}

trait LineItemOps extends Base with Variables with OverloadHack with ParserOps {

  class LineItem

  object LineItem {
    def apply(l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Date], l_commitdate: Rep[Date], l_receiptdate: Rep[Date], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]) = lineitem_obj_new(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    def parse(input: Rep[String], sep: Rep[String]): Rep[LineItem] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[LineItem] = {
      LineItem(input(0).toInt, input(1).toInt, input(2).toInt, input(3).toInt, input(4).toDouble, input(5).toDouble, input(6).toDouble, input(7).toDouble, input(8).toChar, input(9).toChar, input(10).toDate, input(11).toDate, input(12).toDate, input(13), input(14), input(15))
    }
  }

  implicit def repLineItemToLineItemOps(x: Rep[LineItem]) = new lineitemOpsCls(x)
  class lineitemOpsCls(__x: Rep[LineItem]) {
    def l_orderkey = lineitem_l_orderkey(__x)
    def l_partkey = lineitem_l_partkey(__x)
    def l_suppkey = lineitem_l_suppkey(__x)
    def l_linenumber = lineitem_l_linenumber(__x)
    def l_quantity = lineitem_l_quantity(__x)
    def l_extendedprice = lineitem_l_extendedprice(__x)
    def l_discount = lineitem_l_discount(__x)
    def l_tax = lineitem_l_tax(__x)
    def l_returnflag = lineitem_l_returnflag(__x)
    def l_linestatus = lineitem_l_linestatus(__x)
    def l_shipdate = lineitem_l_shipdate(__x)
    def l_commitdate = lineitem_l_commitdate(__x)
    def l_receiptdate = lineitem_l_receiptdate(__x)
    def l_shipinstruct = lineitem_l_shipinstruct(__x)
    def l_shipmode = lineitem_l_shipmode(__x)
    def l_comment = lineitem_l_comment(__x)
  }

  //object defs
  def lineitem_obj_new(l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Date], l_commitdate: Rep[Date], l_receiptdate: Rep[Date], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]): Rep[LineItem]

  //class defs
  def lineitem_l_orderkey(__x: Rep[LineItem]): Rep[Int]
  def lineitem_l_partkey(__x: Rep[LineItem]): Rep[Int]
  def lineitem_l_suppkey(__x: Rep[LineItem]): Rep[Int]
  def lineitem_l_linenumber(__x: Rep[LineItem]): Rep[Int]
  def lineitem_l_quantity(__x: Rep[LineItem]): Rep[Double]
  def lineitem_l_extendedprice(__x: Rep[LineItem]): Rep[Double]
  def lineitem_l_discount(__x: Rep[LineItem]): Rep[Double]
  def lineitem_l_tax(__x: Rep[LineItem]): Rep[Double]
  def lineitem_l_returnflag(__x: Rep[LineItem]): Rep[Char]
  def lineitem_l_linestatus(__x: Rep[LineItem]): Rep[Char]
  def lineitem_l_shipdate(__x: Rep[LineItem]): Rep[Date]
  def lineitem_l_commitdate(__x: Rep[LineItem]): Rep[Date]
  def lineitem_l_receiptdate(__x: Rep[LineItem]): Rep[Date]
  def lineitem_l_shipinstruct(__x: Rep[LineItem]): Rep[String]
  def lineitem_l_shipmode(__x: Rep[LineItem]): Rep[String]
  def lineitem_l_comment(__x: Rep[LineItem]): Rep[String]
}

trait LineItemOpsExp extends LineItemOps with StructExp with EffectExp with BaseFatExp {
  def lineitem_obj_new(l_orderkey: Exp[Int], l_partkey: Exp[Int], l_suppkey: Exp[Int], l_linenumber: Exp[Int], l_quantity: Exp[Double], l_extendedprice: Exp[Double], l_discount: Exp[Double], l_tax: Exp[Double], l_returnflag: Exp[Char], l_linestatus: Exp[Char], l_shipdate: Exp[Date], l_commitdate: Exp[Date], l_receiptdate: Exp[Date], l_shipinstruct: Exp[String], l_shipmode: Exp[String], l_comment: Exp[String]) = struct[LineItem]("LineItem" :: Nil, ListMap("l_orderkey" -> l_orderkey, "l_partkey" -> l_partkey, "l_suppkey" -> l_suppkey, "l_linenumber" -> l_linenumber, "l_quantity" -> l_quantity, "l_extendedprice" -> l_extendedprice, "l_discount" -> l_discount, "l_tax" -> l_tax, "l_returnflag" -> l_returnflag, "l_linestatus" -> l_linestatus, "l_shipdate" -> l_shipdate, "l_commitdate" -> l_commitdate, "l_receiptdate" -> l_receiptdate, "l_shipinstruct" -> l_shipinstruct, "l_shipmode" -> l_shipmode, "l_comment" -> l_comment))
  def lineitem_l_orderkey(__x: Rep[LineItem]) = field[Int](__x, "l_orderkey")
  def lineitem_l_partkey(__x: Rep[LineItem]) = field[Int](__x, "l_partkey")
  def lineitem_l_suppkey(__x: Rep[LineItem]) = field[Int](__x, "l_suppkey")
  def lineitem_l_linenumber(__x: Rep[LineItem]) = field[Int](__x, "l_linenumber")
  def lineitem_l_quantity(__x: Rep[LineItem]) = field[Double](__x, "l_quantity")
  def lineitem_l_extendedprice(__x: Rep[LineItem]) = field[Double](__x, "l_extendedprice")
  def lineitem_l_discount(__x: Rep[LineItem]) = field[Double](__x, "l_discount")
  def lineitem_l_tax(__x: Rep[LineItem]) = field[Double](__x, "l_tax")
  def lineitem_l_returnflag(__x: Rep[LineItem]) = field[Char](__x, "l_returnflag")
  def lineitem_l_linestatus(__x: Rep[LineItem]) = field[Char](__x, "l_linestatus")
  def lineitem_l_shipdate(__x: Rep[LineItem]) = field[Date](__x, "l_shipdate")
  def lineitem_l_commitdate(__x: Rep[LineItem]) = field[Date](__x, "l_commitdate")
  def lineitem_l_receiptdate(__x: Rep[LineItem]) = field[Date](__x, "l_receiptdate")
  def lineitem_l_shipinstruct(__x: Rep[LineItem]) = field[String](__x, "l_shipinstruct")
  def lineitem_l_shipmode(__x: Rep[LineItem]) = field[String](__x, "l_shipmode")
  def lineitem_l_comment(__x: Rep[LineItem]) = field[String](__x, "l_comment")
}

trait N2Ops extends Base with Variables with OverloadHack with ParserOps {

  class N2

  object N2 {
    def apply(n2id: Rep[String], n2junk: Rep[Int]) = n2_obj_new(n2id, n2junk)
    def parse(input: Rep[String], sep: Rep[String]): Rep[N2] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[N2] = {
      N2(input(0), input(1).toInt)
    }
  }

  implicit def repN2ToN2Ops(x: Rep[N2]) = new n2OpsCls(x)
  class n2OpsCls(__x: Rep[N2]) {
    def n2id = n2_n2id(__x)
    def n2junk = n2_n2junk(__x)
  }

  //object defs
  def n2_obj_new(n2id: Rep[String], n2junk: Rep[Int]): Rep[N2]

  //class defs
  def n2_n2id(__x: Rep[N2]): Rep[String]
  def n2_n2junk(__x: Rep[N2]): Rep[Int]
}

trait N2OpsExp extends N2Ops with StructExp with EffectExp with BaseFatExp {
  def n2_obj_new(n2id: Exp[String], n2junk: Exp[Int]) = struct[N2]("N2" :: Nil, ListMap("n2id" -> n2id, "n2junk" -> n2junk))
  def n2_n2id(__x: Rep[N2]) = field[String](__x, "n2id")
  def n2_n2junk(__x: Rep[N2]) = field[Int](__x, "n2junk")
}

trait UserOps extends Base with Variables with OverloadHack with ParserOps {

  class User

  object User {
    def apply(userId: Rep[Int], name: Rep[String], age: Rep[Int]) = user_obj_new(userId, name, age)
    def parse(input: Rep[String], sep: Rep[String]): Rep[User] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[User] = {
      User(input(0).toInt, input(1), input(2).toInt)
    }
  }

  implicit def repUserToUserOps(x: Rep[User]) = new userOpsCls(x)
  class userOpsCls(__x: Rep[User]) {
    def userId = user_userId(__x)
    def name = user_name(__x)
    def age = user_age(__x)
  }

  //object defs
  def user_obj_new(userId: Rep[Int], name: Rep[String], age: Rep[Int]): Rep[User]

  //class defs
  def user_userId(__x: Rep[User]): Rep[Int]
  def user_name(__x: Rep[User]): Rep[String]
  def user_age(__x: Rep[User]): Rep[Int]
}

trait UserOpsExp extends UserOps with StructExp with EffectExp with BaseFatExp {
  def user_obj_new(userId: Exp[Int], name: Exp[String], age: Exp[Int]) = struct[User]("User" :: Nil, ListMap("userId" -> userId, "name" -> name, "age" -> age))
  def user_userId(__x: Rep[User]) = field[Int](__x, "userId")
  def user_name(__x: Rep[User]) = field[String](__x, "name")
  def user_age(__x: Rep[User]) = field[Int](__x, "age")
}

trait AddressOps extends Base with Variables with OverloadHack with ParserOps {

  class Address

  object Address {
    def apply(userId: Rep[Int], street: Rep[String], zip: Rep[Int], city: Rep[String]) = address_obj_new(userId, street, zip, city)
    def parse(input: Rep[String], sep: Rep[String]): Rep[Address] = fromArray(input.split(sep))

    def fromArray(input: Rep[Array[String]]): Rep[Address] = {
      Address(input(0).toInt, input(1), input(2).toInt, input(3))
    }
  }

  implicit def repAddressToAddressOps(x: Rep[Address]) = new addressOpsCls(x)
  class addressOpsCls(__x: Rep[Address]) {
    def userId = address_userId(__x)
    def street = address_street(__x)
    def zip = address_zip(__x)
    def city = address_city(__x)
  }

  //object defs
  def address_obj_new(userId: Rep[Int], street: Rep[String], zip: Rep[Int], city: Rep[String]): Rep[Address]

  //class defs
  def address_userId(__x: Rep[Address]): Rep[Int]
  def address_street(__x: Rep[Address]): Rep[String]
  def address_zip(__x: Rep[Address]): Rep[Int]
  def address_city(__x: Rep[Address]): Rep[String]
}

trait AddressOpsExp extends AddressOps with StructExp with EffectExp with BaseFatExp {
  def address_obj_new(userId: Exp[Int], street: Exp[String], zip: Exp[Int], city: Exp[String]) = struct[Address]("Address" :: Nil, ListMap("userId" -> userId, "street" -> street, "zip" -> zip, "city" -> city))
  def address_userId(__x: Rep[Address]) = field[Int](__x, "userId")
  def address_street(__x: Rep[Address]) = field[String](__x, "street")
  def address_zip(__x: Rep[Address]) = field[Int](__x, "zip")
  def address_city(__x: Rep[Address]) = field[String](__x, "city")
}

trait ApplicationOps extends PartSupplierOps with LogEntryOps with PageCountEntryOps with PartOps with N1Ops with RegionOps with CustomerOps with OrderOps with SupplierOps with NationOps with LineItemOps with N2Ops with UserOps with AddressOps
trait ApplicationOpsExp extends PartSupplierOpsExp with LogEntryOpsExp with PageCountEntryOpsExp with PartOpsExp with N1OpsExp with RegionOpsExp with CustomerOpsExp with OrderOpsExp with SupplierOpsExp with NationOpsExp with LineItemOpsExp with N2OpsExp with UserOpsExp with AddressOpsExp
