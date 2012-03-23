package generated

import scala.collection.mutable.Buffer

object Micro extends App {

  trait N1 {
    def f1: Int = throw new RuntimeException()
    def f2: String = throw new RuntimeException()
    //    def f3 : = throw new RuntimeException()
  }

  class N1_1(override val f1: Int, override val f2: String) extends N1
  case class N1_1C(override val f1: Int, override val f2: String) extends N1

  class N1_N(val f1: Int, val f2: String)

  type X = N1_1C
  type XCasted = N1
  val ar = new Array[XCasted](1000000)

  var x = 3
  var y = "tyii"
  var i = 0
  while (i < ar.length) {
    ar(i) = new X(i, "asdf")
    i += 1
  }
  println("Objects allocated")
  var j = 0
  var times = Buffer[Long]()
  while (j < 20) {
    var start1 = System.nanoTime
    i = 0
    while (i < ar.length) {
      x = ar(i).f1
      y = ar(i).f2
      i += 1
    }
    val time = (System.nanoTime - start1) / 1000
    times += time
    println("time " + (time) + " " + x + " " + y)
    j += 1
  }
  val lowest5 = times.sorted.take(5)

  println(lowest5.sum / 5.0)
}