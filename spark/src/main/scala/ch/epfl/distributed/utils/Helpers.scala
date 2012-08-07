package ch.epfl.distributed.utils

import spark.Partitioner

object Helpers {

  class ClosurePartitioner[K](f: (K, Int) => Int, override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any) = {
      f(key.asInstanceOf[K], numPartitions)
    }
  }

  def makePartitioner[K <: Any](f: (K, Int) => Int, numSplits: Int) = {
    new ClosurePartitioner(f, numSplits)
  }

}