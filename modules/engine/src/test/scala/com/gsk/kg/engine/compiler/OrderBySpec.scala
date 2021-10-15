package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OrderBySpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with ORDER BY" should {

    "execute and obtain expected results" when {

      "no order modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Charlie\""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY ?name
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Alice\""),
          Row("\"Bob\""),
          Row("\"Charlie\"")
        )
      }

      "ASC modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charlie",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY ASC(?name)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Alice\""),
          Row("\"Bob\""),
          Row("\"Charlie\"")
        )
      }

      "DESC modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charlie",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY DESC(?name)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Charlie\""),
          Row("\"Bob\""),
          Row("\"Alice\"")
        )
      }

      "multiple comparators" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Alice",
            ""
          ),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/age>",
            "10",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Charlie",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/age>",
            "30",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Bob",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/age>",
            "20",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name ; foaf:age ?age }
            |ORDER BY ?name DESC(?age)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"A. Alice\""),
          Row("\"A. Bob\""),
          Row("\"A. Charlie\"")
        )
      }

      "multiple comparators 2" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Alice",
            ""
          ),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/age>",
            "10",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Charlie",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/age>",
            "30",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Bob",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/age>",
            "20",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name ; foaf:age ?age }
            |ORDER BY DESC(?name) ?age DESC(?age) ASC(?name) DESC((isBlank(?x) || isBlank(?age)))
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"A. Charlie\""),
          Row("\"A. Bob\""),
          Row("\"A. Alice\"")
        )
      }

      "order DESC" when {

        "alphabetic" in {

          val df: DataFrame = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"11\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"101\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?s ?age
              |WHERE {
              |  ?s foaf:age ?age .
              |}
              |ORDER BY DESC(?age)
              |""".stripMargin

          val result =
            Compiler.compile(df, query, config)

          result.right.get.collect().length shouldEqual 2
          result.right.get.collect.toList shouldEqual List(
            Row("<http://example.org/Alice>", "\"11\""),
            Row("<http://example.org/Bob>", "\"101\"")
          )
        }

        "numeric" in {

          val df: DataFrame = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"101\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?s ?age
              |WHERE {
              |  ?s foaf:age ?age .
              |}
              |ORDER BY DESC(?age)
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().length shouldEqual 2
          result.right.get.collect.toList shouldEqual List(
            Row(
              "<http://example.org/Bob>",
              "\"101\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            ),
            Row(
              "<http://example.org/Alice>",
              "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            )
          )
        }
      }

      "order ASC" when {

        "alphabetic" in {

          val df: DataFrame = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"11\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"101\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?s ?age
              |WHERE {
              |  ?s foaf:age ?age .
              |}
              |ORDER BY ASC(?age)
              |""".stripMargin

          val result =
            Compiler.compile(df, query, config)

          result.right.get.collect().length shouldEqual 2
          result.right.get.collect.toList shouldEqual List(
            Row("<http://example.org/Bob>", "\"101\""),
            Row("<http://example.org/Alice>", "\"11\"")
          )
        }

        "numeric" in {

          val df: DataFrame = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.com/foaf/0.1/age>",
              "\"101\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?s ?age
              |WHERE {
              |  ?s foaf:age ?age .
              |}
              |ORDER BY ASC(?age)
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().length shouldEqual 2
          result.right.get.collect.toList shouldEqual List(
            Row(
              "<http://example.org/Alice>",
              "\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            ),
            Row(
              "<http://example.org/Bob>",
              "\"101\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            )
          )
        }
      }
    }
  }
}
