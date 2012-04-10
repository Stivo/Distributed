package ch.epfl.distributed.datastruct

object Date {
  def apply(year: Int, month: Int, day: Int): Date = new SimpleDate(year, month, day)
  def apply(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) = DateTime(year, month, day, hour, minute, second)
  def apply(s: String): Date = {
    val tokens = s.split("-")
    assert(tokens.size == 3, "expected 3 tokens in date, got: " + tokens.size+" in "+s)
    Date(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
  }
}

abstract class Date(val year: Int, val month: Int, val day: Int) extends Serializable {
  //Intervals
  def +(interval: Interval): Date

  //Comparisons
  def <=(that: Date) = {
    if (year != that.year)
      year < that.year
    else if (month != that.month)
      month < that.month
    else day <= that.day
  }

  def <(that: Date) = (year < that.year) || (year == that.year && (month < that.month || (month == that.month && day < that.day)))

  def ni = throw new RuntimeException("not implemented")
}

case class SimpleDate(_year: Int, _month: Int, _day: Int) extends Date(_year, _month, _day) {
  assert(month > 0 && month <= 12, "invalid month in date")
  assert(day > 0 && day <= 31, "invalid day in date")

  def +(interval: Interval) = {
    import java.util.Calendar
    val date = new java.util.GregorianCalendar(year, month, day)
    date.add(Calendar.YEAR, interval.years)
    date.add(Calendar.MONTH, interval.months)
    date.add(Calendar.DAY_OF_MONTH, interval.days)
    new SimpleDate(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DAY_OF_MONTH))
  }

  override def toString() = {
    val sb = new StringBuilder
    sb append (year.toString) append ("-") append (month.toString) append ("-") append (day.toString)
    sb.toString
  }
}

case class DateTime(_year: Int, _month: Int, _day: Int, hour: Int, minute: Int, second: Int) extends Date(_year, _month, _day) {

  def +(interval: Interval) = ni

}

/**
 * Defines an interval which is a common SQL type
 */
object Interval {
  def apply(n: Int) = new IntervalBuilder(n)
}

class IntervalBuilder(n: Int) {
  def years: Interval = new Interval(n, 0, 0)
  def months: Interval = new Interval(0, n, 0)
  def days: Interval = new Interval(0, 0, n)
}

class Interval(val years: Int, val months: Int, val days: Int) extends Serializable