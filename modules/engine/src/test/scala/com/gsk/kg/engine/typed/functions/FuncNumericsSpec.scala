package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine.typed.functions.TypedLiterals.isDoubleNumericLiteral

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncNumericsSpec
    extends AnyWordSpec
      with Matchers
      with SparkSpec
      with ScalaCheckDrivenPropertyChecks {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  val inColName = "in"
  val ceilExpectedColName = "ceilExpected"
  val roundExpectedColName = "roundExpected"
  val randExpectedColName = "randExpected"
  val absExpectedColName = "absExpected"
  val floorExpectedColName = "floorExpected"
  val nullValue = null

  lazy val elems = List(
    ("1.1", "2", "1", "1.1", "1"),
    ("1.4", "2", "1", "1.4", "1"),
    ("-0.3", "0", "0", "0.3", "-1"),
    ("1.8", "2", "2", "1.8", "1"),
    ("10.5", "11", "11", "10.5", "10"),
    ("-10.5", "-10", "-11", "10.5", "-11")
  )

  lazy val df =
    elems.toTypedDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      absExpectedColName,
      floorExpectedColName
    )

  lazy val typedElems = List[(String, String, String, String, String)](
    (
      "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    ),
    (
      "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    ),
    (
      "\"-0.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"0.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    ),
    (
      "\"10.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
      "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"10.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    ),
    (
      "\"-10.5\"^^<http://www.w3.org/2001/XMLSchema#double>",
      "\"-10\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"-11\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"10.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "\"-11\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    ),
    (
      "\"-10.5\"^^<http://www.w3.org/2001/XMLSchema#string>",
      nullValue,
      nullValue,
      nullValue,
      nullValue
    ),
    ("2.8", "3", "3", "2.8", "2"),
    ("2", "2", "2", "2.0", "2")
  )
  lazy val typedDf =
    typedElems.toTypedDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      absExpectedColName,
      floorExpectedColName
    )

  "FuncNumerics" when {

    "ceil function" should {

      "ceil function returns the smallest integer not smaller than" in {
        df.show(false)

        eval(df, FuncNumerics.ceil, ceilExpectedColName)
      }

      "multiple numeric types" in {
        eval(typedDf, FuncNumerics.ceil, ceilExpectedColName)
      }
    }

    "round function" should {
      "round function returns the smallest integer not smaller than" in {
        eval(df, FuncNumerics.round, roundExpectedColName)
      }

      "multiple numeric types" in {
        eval(typedDf, FuncNumerics.round, roundExpectedColName)
      }
    }

    "rand function" should {

      "rand function" in {
        eval(df, FuncNumerics.rand)
      }

      "rand with multiple numeric types" in {
        eval(typedDf, FuncNumerics.rand)
      }
    }

    "floor function" should {

      "floor function returns the largest integer (closest to positive infinity)" in {
        eval(df, FuncNumerics.floor, floorExpectedColName)
      }

      "floor with multiple numeric types" in {
        eval(typedDf, FuncNumerics.floor, floorExpectedColName)
      }
    }

    "abs function" should {

      "abs function" in {
        eval(df, FuncNumerics.abs, absExpectedColName)
      }

      "abs with multiple numeric types" in {
        eval(typedDf, FuncNumerics.abs, absExpectedColName)
      }
    }
  }

  private def eval(
                    df: DataFrame,
                    f: Column => Column,
                    expectedColName: String
                  ): Assertion = {
    val dfR = df.select(f(col(inColName)))
    val expected = df.select(expectedColName)
    dfR.collect().toList shouldEqual expected.collect().toList
  }

  private def eval(
                    df: DataFrame,
                    f: Column
                  ): Assertion = {
    val dfR = df
      .select(f.as("r"))
      .select(
        isDoubleNumericLiteral(col("r")) &&
          col("r").value.cast(DoubleType).isNotNull
      )
    val expected = Set(Row(true))
    dfR.collect().toSet shouldEqual expected
  }
}
