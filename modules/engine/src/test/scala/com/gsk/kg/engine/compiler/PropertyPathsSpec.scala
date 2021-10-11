package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import scala.annotation.tailrec

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PropertyPathsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  implicit val rowOrdering = new Ordering[Row] {
    override def compare(x: Row, y: Row): Int = {
      val a = x.toSeq.toList.map(_.asInstanceOf[String])
      val b = y.toSeq.toList.map(_.asInstanceOf[String])

      @tailrec
      def step(a: List[String], b: List[String]): Int = {
        (a, b) match {
          case ((ah :: at), (bh :: bt)) if ah == bh => step(at, bt)
          case ((ah :: at), (bh :: bt)) if ah > bh  => 1
          case ((ah :: at), (bh :: bt)) if ah < bh  => -1
          case (Nil, Nil)                           => 0
          case (_, Nil)                             => 1
          case (Nil, _)                             => -1
        }
      }

      step(a, b)
    }
  }

  "Property Paths" when {

    "perform on simple queries" when {

      "alternative | property path" when {

        "no reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows|foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "left reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "right reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|^foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "<http://example.org/Charles>")
          )
        }

        "both reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("\"Charles\"", "<http://example.org/Charles>")
          )
        }

        "with default graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows|foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "with named graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows|foaf:name ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows|foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }
      }

      "sequence / property path" when {

        "two seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:name ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Daniel>", "\"Charles\"")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:knows) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              )
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Daniel>", "\"Charles\"")
            )
          }
        }

        "three seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:friend/foaf:name ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first and second reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }
        }

        "four seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:friend/foaf:parent/foaf:employee ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and second reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, second and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second, third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }
        }

        "with default graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows/foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "\"Charles\"")
          )
        }

        "with named graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows/foaf:name ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "\"Charles\"")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows/foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "\"Charles\"")
          )
        }
      }

      "reverse ^ property path" when {

        "simple query" in {

          val df = List(
            (
              "<http://example.org/alice>",
              "<http://xmlns.org/foaf/0.1/mbox>",
              "<mailto:alice@example.org>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?o ?s
              |WHERE {
              | ?o ^foaf:mbox ?s .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<mailto:alice@example.org>", "<http://example.org/alice>")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/alice>",
              "<http://xmlns.org/foaf/0.1/mbox>",
              "<mailto:alice@example.org>",
              "<http://graph.org/graph1>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?o ?s
              |WHERE {
              | ?o ^foaf:mbox ?s .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<mailto:alice@example.org>", "<http://example.org/alice>")
          )
        }
      }

      "arbitrary length + property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>")
          )
        }

        "with default graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>")
          )
        }

        "with named graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows+ ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>")
          )
        }
      }

      "arbitrary length * property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          )
        }

        "with default graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          )
        }

        "with named graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows* ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          val rows = result.right.get.collect.toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "optional ? property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>")
          )
        }

        "with default graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>")
          )
        }

        "with named graph" in {

          val df = List(
            // graph1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            // graph2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows? ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>")
          )
        }
      }

      "negated ! property path" when {

        "simple uri predicate" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "\"Charles\""
            )
          )
        }

        "nested property path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:name{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            )
          )
        }

        "with default graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s !foaf:knows ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "\"Charles\""
            )
          )
        }

        "with named graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s !foaf:knows ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "\"Charles\""
            )
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "\"Charles\""
            )
          )
        }
      }

      "fixed length {n,m} property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{1, 3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
          )
        }

        "with default graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows{1, 3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
          )
        }

        "with named graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows{1, 3} ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph3>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{1, 3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
          )
        }
      }

      "fixed length {n,} property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with default graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows{2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with named graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows{2,} ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph3>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }
      }

      "fixed length {,n} property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "\"Charles\"",
              "\"Charles\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Alice>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Erick>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with default graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows{,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "\"Charles\"",
              "\"Charles\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Alice>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Erick>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with named graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.org/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows{,2} ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "\"Charles\"",
              "\"Charles\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Alice>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Erick>",
              "<http://example.org/Erick>"
            )
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph3>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row(
              "\"Charles\"",
              "\"Charles\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Alice>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Bob>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Erick>",
              "<http://example.org/Erick>"
            )
          )
        }
      }

      "fixed length {n} property path" when {

        "simple path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with default graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM <http://graph.org/graph1>
              |WHERE {
              | ?s foaf:knows{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "with named graph" in {

          val df = List(
            // Graph 1
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph1>"
            ),
            // Graph 2
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph2>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |FROM NAMED <http://graph.rog/graph1>
              |FROM NAMED <http://graph.org/graph2>
              |WHERE {
              | GRAPH <http://graph.org/graph1> { ?s foaf:knows{2} ?o . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }

        "inclusive mode" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "<http://graph.org/graph1>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "<http://graph.org/graph2>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "<http://graph.org/graph3>"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(
            df,
            query,
            config.copy(isDefaultGraphExclusive = false)
          )

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            )
          )
        }
      }
    }

    "perform on complex queries" when {

      "mix |" when {

        "with /" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows/foaf:name)|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:knows/foaf:name)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows/foaf:name)|(foaf:knows/foaf:name)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }
        }

        "with +" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows+)|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
              Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Erick>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name+)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }
        }

        "with *" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows*)|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
              Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Erick>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name*)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>")
            )
          }
        }

        "with ?" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows?)|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name?)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>")
            )
          }
        }

        "with ^" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Charles>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("\"Charles\"", "<http://example.org/Charles>"),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows|^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
              Row("\"Charles\"", "<http://example.org/Charles>"),
              Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Charles>")
            )
          }
        }

        "with !" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (!foaf:knows|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Charles>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|!foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (!foaf:knows|!foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }
        }

        "with {n,m}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{1,3})|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
              Row("<http://example.org/Alice>", "<http://example.org/Daniel>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name{0,2})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>")
            )
          }
        }

        "with {n,}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{2,})|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
              Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Erick>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name{0,})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            val rows = result.right.get
              .collect()
              .toSeq
              .sorted

            val expectedRows = Seq(
              Row("\"Charles\"", "\"Charles\""),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Erick>", "<http://example.org/Erick>")
            ).sorted

            rows should contain theSameElementsAs expectedRows
          }
        }

        "with {,n}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{,2})|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name{,0})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
              Row("\"Charles\"", "\"Charles\""),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              ),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Alice>", "<http://example.org/Alice>")
            )
          }
        }

        "with {n}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{3})|foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Daniel>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows|(foaf:name{1})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Alice>", "<http://example.org/Bob>")
            )
          }
        }
      }

      "mix /" when {

        "with |" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows|foaf:name)/foaf:knows) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Alice>", "<http://example.org/Charles>")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name|foaf:friend)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows|foaf:friend)/(foaf:name|foaf:name)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }
        }

        "with +" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows+)/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name+)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }
        }

        "with *" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows*)/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name*)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Charles>", "<http://example.org/Daniel>")
            )
          }
        }

        "with ?" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows?)/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name?)) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Charles>", "<http://example.org/Daniel>")
            )
          }
        }

        "with ^" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Daniel>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq()
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq()
          }
        }

        "with !" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (!foaf:knows/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq()
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/!foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
              Row("<http://example.org/Charles>", "<http://example.org/Erick>")
            )
          }

          "both" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (!foaf:knows/!foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq()
          }
        }

        "with {n,m}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{1,3})/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name{1,3})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }
        }

        "with {n,}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{2,})/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name{0,})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            val rows = result.right.get
              .collect()
              .toSeq
              .sorted
            val expectedRows = Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Daniel>"
              ),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
            ).sorted

            rows should contain theSameElementsAs expectedRows
          }
        }

        "with {,n}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{,1})/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Charles>", "\"Charles\""),
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name{,1})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Charles>", "<http://example.org/Daniel>")
            )
          }
        }

        "with {n}" when {

          "left" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ((foaf:knows{2})/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "right" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Erick>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/(foaf:name{0})) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get
              .collect()
              .toSeq should contain theSameElementsAs Seq(
              Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
              Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
              Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
              Row("<http://example.org/Charles>", "<http://example.org/Daniel>")
            )
          }
        }
      }

      "mix +" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name)+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Daniel>"
            ),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "\"Charles\"")
          )
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name)+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "\"Charles\"")
          )
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+)* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+)? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows+) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect.toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+){1,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+){2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)+){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix *" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name)* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "\"Charles\"")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name)* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*)+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            // Path 1
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*)? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows*) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*){0,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            // Path 0
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            // Path 1
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            // Path 2
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*){0,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            // Path 0
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            // Path 1
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            // Path 2
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            // Path 3
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            // Path 4
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*){,1} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows*){1} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix ?" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name)? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name)? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows)?)* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows?)+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows?) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>")
          )
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows?){0,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows?){2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows?){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows?){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix ^" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>")
          )
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows/foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("\"Charles\"", "<http://example.org/Bob>")
          )
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows*) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>")
          )
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows+) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>")
          )
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows?) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>")
          )
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(!foaf:knows) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect.toSeq
          val expectedRows = Seq(
            Row("\"Charles\"", "<http://example.org/Charles>")
          )

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{1,3}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          )
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{2,}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          )
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{,2}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>")
          )
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{2}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          )
        }
      }

      "mix !" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !(foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq()
        }

        // TODO: Fix this one
        "with ^" ignore {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !(^foaf:knows) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect.toSeq
          val expectedRows = Seq(
            Row("\"Charles\"", "<http://example.org/Charles>")
          )

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix {n,m}" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name){1,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "\"Charles\"")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name){1,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Bob>", "\"Charles\"")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3})* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3})+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3})? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            // Path 0
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            // Path 1
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{1,3}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows{1,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3}){2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3}){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            // Path 0
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            // Path 1
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            // Path 2
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){1,3}){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix {n,}" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name){3,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "\"Charles\"")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name){3,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows         = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq().sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,})* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,})+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,})? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{3,}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Daniel>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Alice>"),
            Row("<http://example.org/Erick>", "<http://example.org/Bob>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows{2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq()
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,}){1,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,}){,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){3,}){1} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix {,n}" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3})* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3})+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3})? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{,2}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Charles>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Alice>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows{,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq(
            Row("<http://example.org/Erick>", "<http://example.org/Erick>"),
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\""),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>")
          )
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3}){1,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Erick>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3}){2,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){,3}){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }

      "mix {n}" when {

        "with |" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Bob>", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with /" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name){2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with *" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2})* ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with +" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2})+ ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ?" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2})? ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with ^" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows{2}) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Charles>", "<http://example.org/Alice>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Bob>"),
            Row("<http://example.org/Erick>", "<http://example.org/Charles>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with !" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSeq should contain theSameElementsAs Seq()
        }

        "with {n,m}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2}){1,3} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {n,}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2}){1,} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }

        "with {,n}" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ((foaf:knows){2}){,2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          val rows = result.right.get.collect().toSeq.sorted
          val expectedRows = Seq(
            Row("\"Charles\"", "\"Charles\""),
            Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
            Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
            Row("<http://example.org/Alice>", "<http://example.org/Erick>"),
            Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
            Row("<http://example.org/Daniel>", "<http://example.org/Daniel>"),
            Row("<http://example.org/Erick>", "<http://example.org/Erick>")
          ).sorted

          rows should contain theSameElementsAs expectedRows
        }
      }
    }
  }
}
