import java.time.{Duration, Period, ZonedDateTime}

object DateUtils {

  def getDate(date: String): ZonedDateTime = {
    ZonedDateTime.parse(date)
  }

  def addTime(date: ZonedDateTime, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, 0, 0, seconds)
  }

  def addTime(date: ZonedDateTime, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, 0, minutes, seconds)
  }

  def addTime(date: ZonedDateTime, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, hours, minutes, seconds)
  }

  def addTime(date: ZonedDateTime, days: Int, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, days, hours, minutes, seconds)
  }

  def addTime(date: ZonedDateTime, months: Int, days: Int, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, months, days, hours, minutes, seconds)
  }

  def addTime(date: ZonedDateTime, years: Int, months: Int, days: Int, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    val yearsMonthsDays = Period.of(years, months, days)
    val hoursMinutesSeconds = Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds)

    date.plus(yearsMonthsDays).plus(hoursMinutesSeconds)
  }

  def capDate(date: ZonedDateTime, capDate: ZonedDateTime): ZonedDateTime = {
    if (date.isAfter(capDate)) capDate else date
  }
}
