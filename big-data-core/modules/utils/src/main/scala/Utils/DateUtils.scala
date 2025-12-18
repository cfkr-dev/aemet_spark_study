package Utils

import java.time.{Duration, LocalDate, Period, ZonedDateTime}

/**
 * Small date/time helpers built on the `java.time` API.
 *
 * Provides parsing helpers and a flexible `addTime` family to add a combination
 * of years, months, days, hours, minutes and seconds to a `ZonedDateTime`.
 */
object DateUtils {

  /**
   * Parse an ISO-8601 `ZonedDateTime` string.
   *
   * @param date ISO-8601 date-time string
   * @return parsed `ZonedDateTime`
   */
  def getDateZonedDateTime(date: String): ZonedDateTime = {
    ZonedDateTime.parse(date)
  }

  /**
   * Parse an ISO-8601 `LocalDate` string.
   *
   * @param date ISO-8601 date string
   * @return parsed `LocalDate`
   */
  def getDateLocalDate(date: String): LocalDate = {
    LocalDate.parse(date)
  }

  /**
   * Add seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with added seconds
   */
  def addTime(date: ZonedDateTime, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, 0, 0, seconds)
  }

  /**
   * Add minutes and seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param minutes minutes to add
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with added minutes and seconds
   */
  def addTime(date: ZonedDateTime, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, 0, minutes, seconds)
  }

  /**
   * Add hours, minutes and seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param hours hours to add
   * @param minutes minutes to add
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with added hours, minutes and seconds
   */
  def addTime(date: ZonedDateTime, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, 0, hours, minutes, seconds)
  }

  /**
   * Add days, hours, minutes and seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param days days to add
   * @param hours hours to add
   * @param minutes minutes to add
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with added days, hours, minutes and seconds
   */
  def addTime(date: ZonedDateTime, days: Int, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, 0, days, hours, minutes, seconds)
  }

  /**
   * Add months, days, hours, minutes and seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param months months to add
   * @param days days to add
   * @param hours hours to add
   * @param minutes minutes to add
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with added months, days and time
   */
  def addTime(date: ZonedDateTime, months: Int, days: Int, hours: Long, minutes: Long, seconds: Long): ZonedDateTime = {
    this.addTime(date, 0, months, days, hours, minutes, seconds)
  }

  /**
   * Add a combination of years, months, days, hours, minutes and seconds to a `ZonedDateTime`.
   *
   * @param date input `ZonedDateTime`
   * @param years years to add
   * @param months months to add
   * @param days days to add
   * @param hours hours to add
   * @param minutes minutes to add
   * @param seconds seconds to add
   * @return new `ZonedDateTime` with the added period and duration
   */
  def addTime(
    date: ZonedDateTime,
    years: Int,
    months: Int,
    days: Int,
    hours: Long,
    minutes: Long,
    seconds: Long
  ): ZonedDateTime = {
    val yearsMonthsDays = Period.of(years, months, days)
    val hoursMinutesSeconds = Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds)

    date.plus(yearsMonthsDays).plus(hoursMinutesSeconds)
  }

  /**
   * Cap a date to the provided `capDate`.
   *
   * @param date the input `ZonedDateTime`
   * @param capDate the maximum allowed `ZonedDateTime`
   * @return `capDate` if `date` is after it, otherwise `date`
   */
  def capDate(date: ZonedDateTime, capDate: ZonedDateTime): ZonedDateTime = {
    if (date.isAfter(capDate)) capDate else date
  }
}
