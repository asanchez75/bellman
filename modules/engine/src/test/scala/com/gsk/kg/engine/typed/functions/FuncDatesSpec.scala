package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_timestamp

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.syntax._

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncDatesSpec
  extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "FuncDates" when {

    "now function" should {

      val nowColName = "now"
      val startPos = 2

      "now function returns current date" in {
        val df = List(1, 2, 3).toDF()
        val dfCurrentTime = df.select(FuncDates.now.as(nowColName))

        dfCurrentTime
          .select(
            to_timestamp(
              col(nowColName).value
            ).isNotNull
          )
          .collect()
          .toSet shouldEqual Set(Row(true))
      }
    }

    "year function" should {

      val expected = Array(
        Row("\"2011\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"2020\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"2020\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"2011\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"2011\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"2011\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )

      "year function returns year of datetime" in {
        eval(FuncDates.year, expected, getInitialDataFrame())
      }
    }

    "month function" should {

      val expected = Array(
        Row("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"12\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"12\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )

      "month function returns month of datetime" in {
        eval(FuncDates.month, expected, getInitialDataFrame())
      }
    }

    "day function" should {

      val expected = Array(
        Row("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"9\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"9\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )

      "day function returns day of datetime" in {
        eval(FuncDates.day, expected, getInitialDataFrame())
      }
    }

    "hour function" should {

      val expected = Array(
        Row("\"14\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"14\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"14\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"14\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )

      "hour function returns hour of datetime" in {
        eval(FuncDates.hours, expected, getInitialDataFrame())
      }
    }

    "minutes function" should {

      val expected = Array(
        Row("\"45\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"50\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"50\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"45\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"45\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        Row("\"45\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )

      "minutes function returns min of datetime" in {
        eval(FuncDates.minutes, expected, getInitialDataFrame())
      }
    }

    "seconds function" should {

      val expected = Array(
        Row("\"13.815\"^^<http://www.w3.org/2001/XMLSchema#double>"),
        Row("\"24\"^^<http://www.w3.org/2001/XMLSchema#double>"),
        Row("\"24.888\"^^<http://www.w3.org/2001/XMLSchema#double>"),
        Row("\"13\"^^<http://www.w3.org/2001/XMLSchema#double>"),
        Row("\"13.815\"^^<http://www.w3.org/2001/XMLSchema#double>"),
        Row("\"13.815\"^^<http://www.w3.org/2001/XMLSchema#double>")
      )

      "seconds function returns seconds of datetime" in {
        eval(FuncDates.seconds, expected, getInitialDataFrame())
      }
    }

    "timezone function" should {

      val expected = Array(
        Row("\"-PT5H29M\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"),
        Row("\"PT0S\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"),
        Row("\"PT0S\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"),
        Row("\"-PT5H9M\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"),
        Row("\"PT5H9M\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"),
        Row("\"PT5H\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")
      )

      "timezone function returns min of datetime" in {
        eval(FuncDates.timezone, expected, getInitialDataFrame())
      }
    }

    "tz function" should {

      val expected = Array(
        Row("\"-05:29\""),
        Row("\"Z\""),
        Row("\"Z\""),
        Row("\"-05:09\""),
        Row("\"05:09\""),
        Row("\"05:00\"")
      )

      "tz function returns tz of datetime" in {
        eval(FuncDates.tz, expected, getInitialDataFrame())
      }
    }
  }

  private def getInitialDataFrame(): DataFrame =
    List(
      "\"2011-01-10T14:45:13.815-05:29\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
      "\"2020-12-09T01:50:24Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
      "\"2020-12-09T01:50:24.888Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
      "\"2011-01-10T14:45:13-05:09\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
      "\"2011-01-10T14:45:13.815+05:09\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
      "\"2011-01-10T14:45:13.815+05:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    ).toTypedDF("date")

  private def eval(
                    f: Column => Column,
                    expected: Array[Row],
                    df: DataFrame
                  ): Assertion = {
    df
      .select(f(col(df.columns.head)).as("r"))
      .untype
      .collect() shouldEqual expected
  }
}
