package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Row

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.engine.syntax._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncAggSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "FuncAgg.sample" should {

    "return an arbitrary value from the column" in {

      val elems = List(
        "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
      )
      val df = elems.toTypedDF("a")

      elems.toSet should contain(
        df.select(FuncAgg.sample(df("a"))).untype.collect().head.get(0)
      )
    }
  }

  "FuncAgg.count" should {

    "operate correctly on multiple types" in {
      val df = List(
        "1",
        "1.5",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"3.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"4.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "\"4.5\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
        "\"non numeric type\"",
        "\"non numeric type\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.countAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )
    }
  }

  "FuncAgg.avgAgg" should {

    "operate correctly on multiple numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"3.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"4.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "\"4.5\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
        "\"non numeric type\"",
        "\"non numeric type\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.avgAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"2.7125\"^^<http://www.w3.org/2001/XMLSchema#double>")
      )
    }
  }

  "FuncAgg.sumAgg" should {

    "operate correctly on multiple types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"3.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"4.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "\"4.5\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
        "\"non numeric type\"",
        "\"non numeric type\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.sumAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"21.7\"^^<http://www.w3.org/2001/XMLSchema#double>")
      )
    }
  }

  "FuncAgg.minAgg" should {

    "operate correctly on numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"3.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"4.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "\"4.5\"^^<http://www.w3.org/2001/XMLSchema#numeric>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.minAgg(df("v")).as("result")).untype.collect()

      result shouldEqual Array(
        Row("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )
    }

    "operate correctly on string types" in {

      val df = List(
        "\"alice\"",
        "\"bob\"",
        "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
        "\"bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.minAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(Row("\"alice\""))
    }

    "operate correctly mixing types" in {

      val df = List(
        "1.0",
        "\"2.2\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"alice\"",
        "\"bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.minAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"1.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>")
      )
    }
  }

  "FuncAgg.maxAgg" should {

    "operate correctly on numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"3\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "\"2.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"3.5\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"4.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "\"4.5\"^^<http://www.w3.org/2001/XMLSchema#numeric>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.maxAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"4.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>")
      )
    }

    "operate correctly on string types" in {

      val df = List(
        "\"alice\"",
        "\"bob\"",
        "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
        "\"bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.maxAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(Row("\"bob\""))
    }

    "operate correctly mixing types" in {

      val df = List(
        "1.0",
        "\"2.2\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "\"alice\"",
        "\"bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
      ).toTypedDF("v")

      val result =
        df.select(FuncAgg.maxAgg(df("v")).as("result")).untype.collect()

      result.toSet shouldEqual Set(
        Row("\"bob\"")
      )
    }
  }

}
