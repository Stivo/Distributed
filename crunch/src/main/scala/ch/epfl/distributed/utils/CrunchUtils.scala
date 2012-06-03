package ch.epfl.distributed.utils

import com.cloudera.crunch.CombineFn.Aggregator
import java.util.Collections
import org.apache.hadoop.io.Writable
import java.io.DataOutput
import java.io.DataInput
import com.cloudera.crunch.PTable
import com.cloudera.crunch.{ Pair => CPair }
import com.cloudera.crunch.DoFn
import com.cloudera.crunch.`types`.writable.Writables
import org.apache.hadoop.conf.Configuration
import com.cloudera.crunch.Emitter
import scala.collection.mutable
import com.cloudera.crunch.CombineFn

class CombineWrapperOld[T](reduce: Function2[T, T, T]) extends Aggregator[T] {
  var accum: Option[T] = None
  def reset() {
    accum = None
  }

  def update(value: T) {
    if (accum.isDefined) {
      accum = Some(reduce(accum.get, value))
    } else {
      accum = Some(value)
    }
  }

  def results(): java.lang.Iterable[T] = {
    Collections.singleton(accum.get)
  }

}
class CombineWrapper[K, V](reduce: Function2[V, V, V]) extends CombineFn[K, V] {

  @Override
  def process(input: CPair[K, java.lang.Iterable[V]], emitter: Emitter[CPair[K, V]]) {
    val it = input.second.iterator
    if (it.hasNext) {
      var accum: V = it.next
      while (it.hasNext) {
        accum = reduce(accum, it.next)
      }
      emitter.emit(CPair.of(input.first, accum))
    }
  }
}
object JoinHelper {
  def join[TV <: TaggedValue[U, V], K, U <: Writable, V <: Writable](c: Class[TV], left: PTable[K, U], right: PTable[K, V]): PTable[K, CPair[U, V]] = {
    // new Joiner(c, left, right).doJoin()
    val ptf = left.getTypeFamily();
    val ptt = ptf.tableOf(left.getKeyType(), Writables.records(c));

    val j1 = left.parallelDo(new DoFn[CPair[K, U], CPair[K, TV]] {
      var tv: TaggedValue[U, V] = null
      override def configure(conf: Configuration) {
        tv = c.newInstance
        tv.left = true
      }
      def process(x: CPair[K, U], emitter: Emitter[CPair[K, TV]]) {
        tv.v1 = x.second
        emitter.emit(CPair.of(x.first, tv.asInstanceOf[TV]))
      }
    }, ptt)

    val j2 = right.parallelDo(new DoFn[CPair[K, V], CPair[K, TV]] {
      var tv: TaggedValue[U, V] = null
      override def configure(conf: Configuration) {
        tv = c.newInstance
        tv.left = false
      }
      def process(x: CPair[K, V], emitter: Emitter[CPair[K, TV]]) {
        tv.v2 = x.second
        emitter.emit(CPair.of(x.first, tv.asInstanceOf[TV]))
      }
    }, ptt)

    val joined = j1.union(j2)
    val joinedGrouped = joined.groupByKey
    joinedGrouped.parallelDo(
      new DoFn[CPair[K, java.lang.Iterable[TV]], CPair[K, CPair[U, V]]] {
        val left = mutable.Buffer[U]()
        val right = mutable.Buffer[V]()
        def process(input: CPair[K, java.lang.Iterable[TV]], emitter: Emitter[CPair[K, CPair[U, V]]]) {
          left.clear()
          right.clear()
          val it = input.second().iterator
          while (it.hasNext) {
            val tv = it.next()
            if (tv.left)
              left += tv.v1
            else
              right += tv.v2
          }
          for (l <- left; r <- right) {
            val p1 = CPair.of(l, r)
            val p2 = CPair.of(input.first, p1)
            emitter.emit(p2)
          }
        }
      },
      Writables.tableOf(left.getKeyType, Writables.pairs(left.getValueType, right.getValueType)))
  }

}
case class TaggedValue[V1 <: Writable, V2 <: Writable](var left: Boolean, var v1: V1, var v2: V2) extends Writable {
  override def readFields(in: DataInput) {
    left = in.readBoolean
    if (left) {
      v1.readFields(in)
    } else {
      v2.readFields(in)
    }
  }
  override def write(out: DataOutput) {
    out.writeBoolean(left)
    if (left) {
      v1.write(out)
    } else {
      v2.write(out)
    }
  }
}
