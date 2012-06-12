/**
 * ***************************************
 * Emitting Spark Code
 * *****************************************
 */

package dcdsl.generated;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo

object KMeansApp {
  // field reduction: true
  // loop fusion: false
  // inline in loop fusion: true
  // regex patterns pre compiled: true
  // reduce by key: true
  def main(sparkInputArgs: Array[String]) {
    System.setProperty("spark.default.parallelism", "40")
    System.setProperty("spark.local.dir", "/mnt/tmp")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "dcdsl.generated.Registrator_KMeansApp")
    System.setProperty("spark.kryoserializer.buffer.mb", "20")

    val sc = new SparkContext(sparkInputArgs(0), "KMeansApp")

    println(System.currentTimeMillis + " " + """Taking sample""")
    val x1 = sparkInputArgs.drop(1); // First argument is for spark context;
    val x14 = x1(1);
    val x15 = x14.toInt;
    val x2 = x1(0);
    val x3 = sc.textFile(x2);
    val x5 = new ch.epfl.distributed.datastruct.RegexFrontend(""" """, true, true);
    @inline
    def x117(x4: (java.lang.String)) = {
      val x6 = x5.split(x4, 0);
      val x9 = {
        val out = new Array[Double](x6.length)
        val in = x6
        var i = 0
        while (i < in.length) {
          val x7 = in(i)
          val x8 = x7.toDouble;
          out(i) = x8
          i += 1
        }
        out
      }
      val x10 = x9;
      x10: Array[Double]
    }
    val x118 = x3.map(x117);
    val x119 = x118.cache();
    val x120 = x119.takeSample(false, x15, 42);
    val x121 = x120.toArray;
    var x122: Array[Array[Double]] = x121
    var x123: Double = 1.0
    var x124: Int = 0
    println(System.currentTimeMillis + " " + """Starting big while""")
    val x33 = scala.Double.PositiveInfinity;
    @inline
    def x126(x59: scala.Tuple2[Array[Double], Int], x60: scala.Tuple2[Array[Double], Int]) = {
      val x61 = x59._1;
      val x63 = x60._1;
      val x65 = {
        // Vector Simple Op
        if (x61.size != x63.size)
          throw new IllegalArgumentException("Should have same length")
        var out = new Array[Double](x61.size)
        var i = 0
        while (i < x61.size) {
          out(i) = x61(i) + x63(i)
          i += 1
        }
        out
      };
      val x62 = x59._2;
      val x64 = x60._2;
      val x66 = x62 + x64;
      val x67 = (x65, x66);
      x67: scala.Tuple2[Array[Double], Int]
    }
    @inline
    def x127(x72: (scala.Tuple2[Int, scala.Tuple2[Array[Double], Int]])) = {
      val x73 = x72._1;
      val x74 = x72._2;
      val x75 = x74._1;
      val x76 = x74._2;
      val x77 = x76;
      val x78 = {
        // Vector Point wise op

        var out = new Array[Double](x75.size)
        var i = 0
        while (i < x75.size) {
          out(i) = x75(i) / x77
          i += 1
        }
        out
      };
      val x79 = (x73, x78);
      x79: scala.Tuple2[Int, Array[Double]]
    }
    val x185 = while ({
      val x128 = x124;
      val x129 = x128 < 5;
      x129
    }) {
      @inline
      def x156(x28: (Array[Double])) = {
        val x131 = x122;
        var x132: Int = 0
        var x133: Int = 0
        var x134: Double = x33
        var x135: Int = 0
        val x136 = x131.length;
        val x152 = while ({
          val x137 = x135;
          val x138 = x137 < x136;
          x138
        }) {
          val x140 = x135;
          val x141 = x134;
          val x142 = x131(x140);
          val x143 = {
            // Vector Squared Dist
            if (x28.size != x142.size)
              throw new IllegalArgumentException("Should have same length")
            var out = 0.0
            var i = 0
            while (i < x28.size) {
              val dist = x28(i) - x142(i); out += dist * dist
              i += 1
            }
            out
          };
          val x144 = x143 < x141;
          val x148 = if (x144) {
            x134 = x143
            x133 = x140
            ()
          } else {
            ()
          }
          val x149 = x140 + 1;
          x135 = x149
          ()
        }
        val x153 = x133;
        val x29 = (x28, 1);
        val x154 = (x153, x29);
        x154: scala.Tuple2[Int, scala.Tuple2[Array[Double], Int]]
      }
      val x157 = x119.map(x156);
      val x159 = x157.reduceByKey(x126);
      var x160: RDD[scala.Tuple2[Int, scala.Tuple2[Array[Double], Int]]] = x159
      val x161 = x160;
      x123 = 0.0
      val x163 = x161.map(x127);
      val x164 = x163.collect();
      val x165 = x164.toArray;
      val x171 = x165.foreach {
        x85 =>
          val x166 = x122;
          val x88 = x85._2;
          val x87 = x85._1;
          val x167 = x166(x87);
          val x168 = {
            // Vector Squared Dist
            if (x167.size != x88.size)
              throw new IllegalArgumentException("Should have same length")
            var out = 0.0
            var i = 0
            while (i < x167.size) {
              val dist = x167(i) - x88(i); out += dist * dist
              i += 1
            }
            out
          };
          val x169 = x123 += x168;
          ()
      }
      val x172 = x165.toArray;
      val x173 = x172.sortBy({ x95 =>
        val x96 = x95._1;
        x96
      })
      val x174 = {
        val out = new Array[Array[Double]](x173.length)
        val in = x173
        var i = 0
        while (i < in.length) {
          val x99 = in(i)
          val x101 = x99._2;
          out(i) = x101
          i += 1
        }
        out
      }
      x122 = x174
      val x176 = x124;
      val x177 = x123;
      val x178 = """Iteration """ + x176;
      val x179 = x178 + """ done, distance """;
      val x180 = x179 + x177;
      println(System.currentTimeMillis + " " + x180)
      val x182 = x176 + 1;
      x124 = x182
      ()
    }
    println(System.currentTimeMillis + " " + """5 iterations done""")

    System.exit(0)
  }
}
// Types that are used in this program

class Registrator_KMeansApp extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {

    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}
/**
 * ***************************************
 * End of Spark Code
 * *****************************************
 */
