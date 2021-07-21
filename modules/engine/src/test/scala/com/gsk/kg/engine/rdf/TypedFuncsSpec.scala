package com.gsk.kg.engine
package rdf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class TypedFuncsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with TestConfig
    with CommonGenerators {

  import sqlContext.implicits._

  "TypedFuncsSpec" should {

    "run IfSpec fast" in {

      val df = List(
        (
          "<http://example.org/alice>",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Alice\""
        ),
        (
          "<http://example.org/alice>",
          "<http://xmlns.com/foaf/0.1/age>",
          "21"
        ),
        (
          "<http://example.org/bob>",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Bob\""
        ),
        (
          "<http://example.org/bob>",
          "<http://xmlns.com/foaf/0.1/age>",
          "15"
        )
      ).toDF("s", "p", "o")

      val typed = Typer.to(df)

      //  This is the query we're benchmarking against.  You can see the whole test done in IfSpec
      //
      //  |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      //  |
      //  |SELECT ?name ?cat
      //  |WHERE {
      //  | ?s foaf:name ?name .
      //  | ?s foaf:age ?age .
      //  | BIND(IF(?age < 18, "child", "adult") AS ?cat)
      //  |}
      //  |""".stripMargin

      val first = typed
        .filter(typed("p")("value") === "http://xmlns.com/foaf/0.1/name")
        .select(col("s").as("?s"), col("o").as("?name"))

      val second = typed
        .filter(typed("p")("value") === "http://xmlns.com/foaf/0.1/age")
        .select(col("s").as("?s"), col("o").as("?age"))

      val joined =
        first.join(second, "?s")

      val withNewColumn =
        joined.withColumn(
          "?cat",
          TypedFuncs.`if`(
            Typer.createRecord(col("?age")("value") < 18, RdfType.Boolean.repr),
            ifTrue = Typer.createRecord(lit("child"), RdfType.String.repr),
            ifFalse = Typer.createRecord(lit("adult"), RdfType.String.repr)
          )
        )

      Typer
        .from(withNewColumn)
        .select("?name", "?cat")
        .collect
        .toSet shouldEqual Set(
        Row(
          "\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
          "\"adult\"^^<http://www.w3.org/2001/XMLSchema#string>"
        ),
        Row(
          "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>",
          "\"child\"^^<http://www.w3.org/2001/XMLSchema#string>"
        )
      )
    }
  }
}
