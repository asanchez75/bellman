package com.gsk.kg.engine.typed.functions

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.engine.syntax._
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncArithmeticSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Arithmetics" when {

    "FuncArithmetics.add" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "2"),
            ("-4", "1.5", "-2.5"),
            ("1.5e3", "1", "\"1501.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.5", "1.5", "3.0"),
            ("1", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.2", "\"-4.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-3.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1.23", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1.23", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-4.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1000001.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.46\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.untype.collect() shouldEqual expected.untype.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.01\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1499.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "2.0"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.5", "-2.5"),
            ("\"1.5e3\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "\"1501.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.5", "3.0"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-4.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-3.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-4.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1000001.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.46\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.5", "\"-2.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5e3\"^^<http://www.w3.org/2001/XMLSchema#float>", "1", "\"1501.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.5", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.2e5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-4.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"119995.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23e-3\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-5.2322\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-5.23097\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1000001.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.46\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.5", "\"-2.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "1", "\"1501.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.5", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.2e5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-4.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"119995.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23e-3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-5.2322\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-5.23097\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1000001.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.46\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }

    "FuncArithmetics.subtract" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "0"),
            ("1", "1.5", "-0.5"),
            ("1.5", "1e-1", "\"1.4\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.5", "-2.2", "3.7"),
            ("1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1e2", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"98.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.5", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.2", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("2.23", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1e-2", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.22\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("2.23", "\"-1e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1002.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1e-1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.1\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1e2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"103.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-6\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-4.5\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.5", "\"-0.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1e-1", "\"1.4\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "-2.2", "\"3.7\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"100\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"98.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"0.01\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.23e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.0223\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1002.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.5", "\"-0.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "1e-1", "\"\"1.4\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "-2.2", "\"3.7\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1e2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"98.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.3\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.0223\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1002.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.5", "\"-0.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "1e-1", "\"\"1.4\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "-2.2", "\"3.7\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1e2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"98.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.3\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.0223\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1002.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

      }
    }

    "FuncArithmetics.multiply" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "1"),
            ("1", "1.5", "1.5"),
            ("1.5e3", "1", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("3.55", "-2.26", "-8.023"),
            ("1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("1e-2", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"0.02\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("-1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.2", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.0", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("2.23", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1.23", "\"1.23e3\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1512.9\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("2.23e-1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.223\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"0\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"6\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"-12\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"6\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-72.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-2.75\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-11.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"-66\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"7.75\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-511.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-2.12e1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-21.2\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.56e2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"768.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-6\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-5.2377\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"31.4262\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.5", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"3.55\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "-2.26", "\"-8.023\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1e-1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.223\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.5129\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1e2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"223.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.5", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "1", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"3.55\"^^<http://www.w3.org/2001/XMLSchema#float>", "-2.26", "\"-8.023\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1e3\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.5129\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-0.0123\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.5", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "1", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"3.55\"^^<http://www.w3.org/2001/XMLSchema#double>", "-2.26", "\"-8.023\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1e3\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.5129\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-0.0123\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2.23\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }

    "FuncArithmetics.divide" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "1"),
            ("-4", "2", "\"-2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("1.5e3", "1", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.5", "1.5", "1.0"),
            ("1", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.5", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1.2", "\"-1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("1", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1.23", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"123.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1.23", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-0.2351816443594646\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("1", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23E-6\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("1.23", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"6\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            ("\"-12\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"6\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-2.75\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.4545454545454546\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"-66\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"7.75\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-8.516129032258064\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-2.12e1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-0.04716981132075472\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2.56e2\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.01171875\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-6\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-5.2377\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.1455409817286213\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "\"-2.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1e-3", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>","\"0.75\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"123.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-0.2351816443594646\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23E-6\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#float>", "2", "\"-2.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "1e-3", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>","\"0.75\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"123.0\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-0.2351816443594646\"^^<http://www.w3.org/2001/XMLSchema#float>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23E-6\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"-4\"^^<http://www.w3.org/2001/XMLSchema#double>", "2", "\"-2.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "1e-3", "\"1500.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>","\"0.75\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"1.5\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"-1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "\"-1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1e-2\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"123.0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"-5.23\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"-0.2351816443594646\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"0.8130081300813008\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0e6\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23E-6\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            ("\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>")
          ).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")).as("result"))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }
  }
}
