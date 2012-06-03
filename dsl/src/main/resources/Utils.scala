package ch.epfl.distributed.datastruct

import java.util.regex.Pattern

class FastSplitter(matchOn: String) extends Serializable {

  var matchByte: Byte = -1
  var useFinder = true
  var pattern: Pattern = null
  if (matchOn.charAt(0) == '\\' && matchOn.length == 2) {
    matchByte = matchOn.charAt(1).toByte
    //    matchByte = StorageUtil.parseFieldDel(matchOn)
  } else if (matchOn.length == 1) {
    matchByte = matchOn.charAt(0).toByte
    //    matchByte = StorageUtil.parseFieldDel(matchOn)
  } else {
    useFinder = false
    pattern = Pattern.compile(matchOn)
  }
  def split(s: String, max: Int): IndexedSeq[String] = {
    if (useFinder)
      new Finder(s, max)
    else
      pattern.split(s, max)
  }
  class Finder(val s: String, val max: Int) extends IndexedSeq[String] {
    private[this] val byte = matchByte
    private[this] var at = 1
    private[this] var atChar = 0
    private[this] val ranges = Array.ofDim[Int](max)
    private[this] val ends = Array.ofDim[Int](max)
    ends(max - 1) = s.length
    ranges(0) = 0
    def apply(num: Int): String = {
      while (at <= num + 1 && at < max) {
        atChar = s.indexOf(byte, atChar)
        ends(at - 1) = atChar
        atChar += 1
        ranges(at) = atChar
        at += 1
      }
      if (at > num) {
        return s.substring(ranges(num), ends(num))
      }
      throw new RuntimeException("Did not find it")
    }
    def length = max
  }
}
