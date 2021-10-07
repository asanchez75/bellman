package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DaySpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-day
  DAY("2011-01-10T14:45:13.815-05:00"^^xsd:datetime) -> 10
   */

  lazy val df: DataFrame = List(
    (
      "_:a",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2011-01-10T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"
    ),
    (
      "_:b",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2012-04-14T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2013-12-09T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List("\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"14\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"9\"^^<http://www.w3.org/2001/XMLSchema#integer>").map(Row(_))

  val projection: Option[Column] = None

  "perform day function correctly" when {
    "select day response with a day of datetime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT DAY(?date)
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

    "bind day response with a day value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?m
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(day(?date) as ?m)
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
