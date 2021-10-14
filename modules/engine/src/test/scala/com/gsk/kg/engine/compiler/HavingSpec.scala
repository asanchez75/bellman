package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HavingSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  lazy val dfSimple: DataFrame = List(
    ("_:a", "<http://example.org/size>", "5"),
    ("_:a", "<http://example.org/size>", "15"),
    ("_:a", "<http://example.org/size>", "20"),
    ("_:a", "<http://example.org/size>", "7"),
    ("_:b", "<http://example.org/size>", "9.5"),
    ("_:b", "<http://example.org/size>", "1"),
    ("_:b", "<http://example.org/size>", "11")
  ).toDF("s", "p", "o")

  lazy val dfComplete: DataFrame = List(
    (
      "<http://example/phillips>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/phillips>",
      "<http://schema.org/title>",
      "Scala puzzlers",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/odersky>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/odersky>",
      "<http://schema.org/title>",
      "Programming in Scala",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/spoon>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/spoon>",
      "<http://schema.org/title>",
      "Programming in Scala",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/venners>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/venners>",
      "<http://schema.org/title>",
      "Programming in Scala",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/siew>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/siew>",
      "<http://schema.org/title>",
      "Programming in Scala",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/chambers>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/chambers>",
      "<http://schema.org/title>",
      "Spark: The definitive Guide",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/zaharia>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://gsk-kg.rdip.gsk.com/dm/1.0/ReferencedDoc>",
      "<http://id.gsk.com/dataset/elsevier/>"
    ),
    (
      "<http://example/zaharia>",
      "<http://schema.org/title>",
      "Spark: The definitive Guide",
      "<http://id.gsk.com/dataset/elsevier/>"
    )
  ).toDF("s", "p", "o", "g")

  "performs HAVING queries" should {

    "execute and obtain expected results" when {

      "single condition on HAVING clause with COUNT" in {

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x (COUNT(?size) AS ?count)
            |WHERE {
            |  ?x ex:size ?size
            |}
            |GROUP BY ?x
            |HAVING(COUNT(?size) = 4)
            |""".stripMargin

        val result = Compiler.compile(dfSimple, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        )
      }

      "single condition on HAVING clause with AVG" in {

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT (AVG(?size) AS ?asize)
            |WHERE {
            |  ?x ex:size ?size
            |}
            |GROUP BY ?x
            |HAVING(AVG(?size) > 10)
            |""".stripMargin

        val result = Compiler.compile(dfSimple, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"11.75\"^^<http://www.w3.org/2001/XMLSchema#double>")
        )
      }

      "single condicion on HAVING clause with COUNT do a good comparation beetween numbers types with zero rows" in {
        val query =
          """
            |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
            |PREFIX schema: <http://schema.org/>
            |
            |SELECT ?title (COUNT(?title) as ?reference_count)
            |FROM <http://id.gsk.com/dataset/elsevier/>
            |WHERE
            |{
            |       ?refdoc a dm:ReferencedDoc .
            |       ?refdoc schema:title ?title .
            |}
            |
            |GROUP BY ?title
            |HAVING (COUNT(?title) > 19)
            |""".stripMargin

        val result = Compiler.compile(dfComplete, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set.empty[Row]
      }

      "single condicion on HAVING clause with COUNT do a good comparation beetween numbers types with some rows" in {

        val query =
          """
              |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
              |PREFIX schema: <http://schema.org/>
              |
              |SELECT ?title (COUNT(?title) as ?reference_count)
              |FROM <http://id.gsk.com/dataset/elsevier/>
              |WHERE
              |{
              |       ?refdoc a dm:ReferencedDoc .
              |       ?refdoc schema:title ?title .
              |}
              |
              |GROUP BY ?title
              |HAVING (COUNT(?title) > 2)
              |""".stripMargin

        val result = Compiler.compile(dfComplete, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "\"Programming in Scala\"",
            "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>"
          )
        )
      }

      "single condicion on HAVING clause with COUNT do a good comparation beetween numbers types with all the rows" in {

        val query =
          """
            |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
            |PREFIX schema: <http://schema.org/>
            |
            |SELECT ?title (COUNT(?title) as ?reference_count)
            |FROM <http://id.gsk.com/dataset/elsevier/>
            |WHERE
            |{
            |       ?refdoc a dm:ReferencedDoc .
            |       ?refdoc schema:title ?title .
            |}
            |
            |GROUP BY ?title
            |HAVING (COUNT(?title) > 0)
            |""".stripMargin

        val result = Compiler.compile(dfComplete, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "\"Scala puzzlers\"",
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"
          ),
          Row(
            "\"Programming in Scala\"",
            "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>"
          ),
          Row(
            "\"Spark: The definitive Guide\"",
            "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"
          )
        )
      }
    }
  }
}
