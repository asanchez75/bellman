package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class YearSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-year
  YEAR("2011-01-10T14:45:13.815-05:00"^^xsd:dateTime) -> 2011
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
      "\"2012-01-10T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2013-01-10T14:45:13.815-05:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List("\"2011\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2012\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"2013\"^^<http://www.w3.org/2001/XMLSchema#integer>").map(Row(_))

  val projection: Option[Column] = None

  "perform year function correctly" when {
    "select year response with a year of datetime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT YEAR(?date)
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

    "bind year response with a year value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?d
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(year(?date) as ?d)
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
