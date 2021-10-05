package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{month => sMonth, year => sYear, _}
import org.apache.spark.sql.types.IntegerType

import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine.typed.functions.TypedLiterals.isDateTimeLiteral
import com.gsk.kg.engine.typed.functions.TypedLiterals.nullLiteral

object FuncDates {

  private val Seconds = 5

  /** Returns an XSD dateTime value for the current query execution. All calls to this function in any one query
    * execution must return the same value. The exact moment returned is not specified.
    * e.g. "2011-01-10T14:45:13.815-05:00"^^xsd:dateTime
    *
    * @return
    */
  def now: Column =
    RdfType.DateTime(
      date_format(current_timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )

  /** Returns the year part of arg as an integer.
    *
    * @param col
    * @return
    */
  def year(col: Column): Column = apply(sYear, col)

  /** Returns the month part of arg as an integer.
    *
    * @param col
    * @return
    */
  def month(col: Column): Column = apply(sMonth, col)

  /** Returns the day part of arg as an integer.
    *
    * @param col
    * @return
    */
  def day(col: Column): Column = apply(dayofmonth, col)

  /** Returns the hours part of arg as an integer.
    * The value is as given in the lexical form of the XSD dateTime.
    *
    * @param col
    * @return
    */
  def hours(col: Column): Column = {
    val Hours = 3
    getTimeFromDateTimeCol(col, Hours)
  }

  /** Returns the minutes part of the lexical form of arg.
    * The value is as given in the lexical form of the XSD dateTime.
    *
    * @param col
    * @return
    */
  def minutes(col: Column): Column = {
    val Minutes = 4
    getTimeFromDateTimeCol(col, Minutes)
  }

  /** Returns the seconds part of the lexical form of arg.
    *
    * @param col
    * @return
    */
  def seconds(col: Column): Column =
    getTimeFromDateTimeCol(col, Seconds)

  /** Returns the timezone part of arg as an xsd:dayTimeDuration.
    * Raises an error if there is no timezone.
    *
    * @param col
    * @return
    */
  def timezone(col: Column): Column = {

    val timeZone = getTimeZoneComponents(col)
    when(timeZone.value.isNull, nullLiteral)
      .when(timeZone.value.like("Z"), RdfType.DateTime(lit("PT0S")))
      .when(
        timeZone.value.rlike("-[0-9]{1,2}:[0-9]{1,2}"), {
          val PosSign    = 1
          val PosHours   = 2
          val PosMinutes = 5
          buildTimeZone(timeZone, Some(PosSign), PosHours, PosMinutes)
        }
      )
      .when(
        timeZone.value.rlike("[0-9]{1,2}:[0-9]{1,2}"), {
          val PosSign    = None
          val PosHours   = 1
          val PosMinutes = 4
          buildTimeZone(timeZone, PosSign, PosHours, PosMinutes)
        }
      )
  }

  /** Returns the timezone part of arg as a simple literal.
    * Returns the empty string if there is no timezone.
    *
    * @param col
    * @return
    */
  def tz(col: Column): Column = {
    val timeZone = getTimeZoneComponents(col)
    when(timeZone.value.isNull, RdfType.String(lit("")))
      .otherwise(timeZone)
  }

  /** Check if col is a xsd:dateTime type and apply function in case true
    *
    * @param f
    * @param col
    * @return f(col) or lit(null) if col isn't xsd:dateTime type
    */
  private def apply(f: Column => Column, col: Column): Column =
    when(
      isDateTimeLiteral(col),
      RdfType.Int(f(col.value))
    )

  /** Get hours, minutes of dateTime column
    *
    * @param col
    * @param pos
    * @return Column with
    *         Integer if hours or minutes
    *         usage: getTimeFromDateTimeCol(2011-01-10T14:45:13.815-05:29, 3) = 14
    */
  private def getTimeFromDateTimeCol(col: Column, pos: Int): Column = {
    val dateTimeRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"

    val `type` = pos match {
      case Seconds => RdfType.Double
      case _       => RdfType.Int
    }

    when(
      col.value.rlike(dateTimeRegex),
      `type`(
        split(
          regexp_replace(
            col.value,
            "[:TZ+]",
            "-"
          ),
          "-"
        ).getItem(pos)
      )
    ).otherwise(nullLiteral)
  }

  /** Get timezone of a datetime input in multiples formats
    *
    * @param col
    * @return timezone formatted
    *         usage
    *         getTimeZoneComponents(2020-12-09T01:50:24.888Z) = Z
    *         getTimeZoneComponents(2011-01-10T14:45:13.815-05:29) = -05:29
    *         getTimeZoneComponents(2011-01-10T14:45:13.815+05:09) = 05:29
    */
  private def getTimeZoneComponents(col: Column): Column = {
    val dateTimeWithTimeZoneRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{1,3}[+-]{1}[0-9]{1,2}:[0-9]{1,2}"
    val dateTimeWithTimeZoneWithoutDecimalSecondsRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}[+-]{1}[0-9]{1,2}:[0-9]{1,2}"
    val dateTimeWithoutTimeZoneRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(.[0-9]{1,3})?Z"

    val PosTimeZone     = -6
    val PosSign         = 1
    val PosHours        = 2
    val PosMinutes      = 5
    val LenTimeZone     = 6
    val LenSign         = 1
    val LenHoursMinutes = 2

    when(
      col.value.rlike(dateTimeWithTimeZoneRegex) || col.value.rlike(
        dateTimeWithTimeZoneWithoutDecimalSecondsRegex
      ), {
        val timeZone = substring(
          col.value,
          PosTimeZone,
          LenTimeZone
        )
        val sign = substring(timeZone, PosSign, LenSign)
        val hoursTimeZone =
          substring(timeZone, PosHours, LenHoursMinutes)
        val minutesTimeZone =
          substring(timeZone, PosMinutes, LenHoursMinutes)

        val signFormatted = when(sign.like("-"), sign).otherwise(lit(""))

        RdfType.String(
          format_string(
            "%s%s:%s",
            signFormatted,
            hoursTimeZone,
            minutesTimeZone
          )
        )
      }
    ).when(
      col.value.rlike(dateTimeWithoutTimeZoneRegex),
      RdfType.String(lit("Z"))
    ).otherwise(nullLiteral)
  }

  /** Build a timezone depends of the input format
    *
    * @param timeZone
    * @param PosSignOpt
    * @param PosHours
    * @param PosMinutes
    * @return timezone formatted
    *         usage
    *         buildTimeZone(-05:29) = "\"-PT5H29M\"^^xsd:dateTime"
    *         buildTimeZone(05:00)  = "\"PT5H\"^^xsd:dateTime"
    */
  private def buildTimeZone(
      timeZone: Column,
      PosSignOpt: Option[Int],
      PosHours: Int,
      PosMinutes: Int
  ): Column = {
    val LenSign         = 1
    val LenHoursMinutes = 2

    val sign = PosSignOpt
      .map(posSing => substring(timeZone.value, posSing, LenSign))
      .getOrElse(lit(""))
    val signFormatted = when(sign.like("-"), sign).otherwise(lit(""))
    val hoursTimeZone =
      substring(timeZone.value, PosHours, LenHoursMinutes).cast(IntegerType)
    val minutesTimeZone = substring(timeZone.value, PosMinutes, LenHoursMinutes)
    val minutesFormatted =
      when(minutesTimeZone.like("00"), lit("")).otherwise(
        concat(minutesTimeZone.cast(IntegerType), lit("M"))
      )

    RdfType.DateTime(
      format_string(
        "%sPT%sH%s",
        signFormatted,
        hoursTimeZone,
        minutesFormatted
      )
    )
  }
}
