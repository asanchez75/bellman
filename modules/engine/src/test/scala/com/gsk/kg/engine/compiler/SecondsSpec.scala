package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SecondsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-seconds
  SECONDS("2011-01-10T14:45:13.815-05:00"^^xsd:dateTime) -> 13.815
   */

  lazy val df: DataFrame = List(
    (
      "_:a",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2011-01-10T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    ),
    (
      "_:b",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2012-04-10T08:45:13+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2012-04-10T22:45:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List("\"13.815\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"13\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"00\"^^<http://www.w3.org/2001/XMLSchema#double>").map(Row(_))

  val projection: Option[Column] = None

  "perform seconds function correctly" when {
    "select seconds response with a seconds of dateTime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT SECONDS(?date)
          |WHERE  {
          |   ?x foaf:date ?date
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
    }

    "bind seconds response with a seconds value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?h
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(seconds(?date) as ?h)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
    }
  }

}
