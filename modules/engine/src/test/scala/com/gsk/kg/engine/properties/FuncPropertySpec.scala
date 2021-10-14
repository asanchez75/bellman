package com.gsk.kg.engine.properties

import higherkindness.droste.contrib.NewTypesSyntax._
import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.engine.syntax._
import com.gsk.kg.sparqlparser.Result
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncPropertySpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators
    with TestConfig {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Properties" when {

    "Uri function" should {

      "return expected dataframe" in {

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
        ).toTypedDF("s", "p", "o").@@[Untyped]

        val uriFunc = FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

        uriFunc.unwrap.untype.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          )
        )
      }
    }

    "Alternative function" should {

      "return expected column" in {

        val df = List(
          (
            "<http://example.org/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book2>",
            "<http://www.w3.org/2000/01/rdf-schema#label>",
            "From Earth To The Moon"
          ),
          (
            "<http://example.org/book3>",
            "<http://www.w3.org/2000/01/rdf-schema#label2>",
            "Another title"
          )
        ).toTypedDF("s", "p", "o").@@[Untyped]

        lazy val titleUriFunc =
          FuncProperty.uri(df, "<http://purl.org/dc/elements/1.1/title>")
        lazy val labelUriFunc =
          FuncProperty.uri(df, "<http://www.w3.org/2000/01/rdf-schema#label>")
        val alternativeFunc =
          FuncProperty.alternative(titleUriFunc, labelUriFunc)

        val result = alternativeFunc.right.get.unwrap.untype.collect
        result.toSet shouldEqual Set(
          Row(
            "<http://example.org/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "\"SPARQL Tutorial\""
          ),
          Row(
            "<http://example.org/book2>",
            "<http://www.w3.org/2000/01/rdf-schema#label>",
            "\"From Earth To The Moon\""
          )
        )
      }
    }

    "Seq function" should {

      "return expected dataframe" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>",
            ""
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>",
            ""
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\"",
            ""
          )
        ).toTypedDF("s", "p", "o", "g").@@[Untyped]

        lazy val knowsUriFunc =
          FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")
        lazy val nameUriFunc =
          FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/name>")

        import cats.syntax.either._

        val result: Result[DataFrame @@ Untyped] = for {
          // (seq <http://xmlns.org/foaf/0.1/knows> <http://xmlns.org/foaf/0.1/knows>)
          innerSeq <- FuncProperty.seq(
            knowsUriFunc,
            knowsUriFunc,
            config
          )
          // (seq (seq <http://xmlns.org/foaf/0.1/knows> <http://xmlns.org/foaf/0.1/knows>) <http://xmlns.org/foaf/0.1/name>)
          outerSeq <- FuncProperty.seq(
            innerSeq,
            nameUriFunc,
            config
          )
        } yield outerSeq

        result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/Alice>",
            "\"seq:pl/pr\"",
            "\"Charles\"",
            "\"\""
          )
        )
      }
    }

    "BetweenNAndM function" should {

      "return expected values" when {

        "from Some(1) to Some(3) path length (n < m)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 1
          val m = 3

          // ?s foaf:knows{1, 3} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              Some(m),
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(3) to Some(2) path length (n > m)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 3
          val m = 2

          // ?s foaf:knows{3, 2} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              Some(m),
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(0) to Some(2) path length (with zero path length)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 0
          val m = 2

          // ?s foaf:knows{0, 2} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              Some(m),
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              null,
              "<http://example.org/Alice>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              null,
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              null,
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              null,
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Erick>",
              null,
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "\"Charles\"",
              null,
              "\"Charles\"",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(-1) to Some(-1) path length (error because n or m i less that 0)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = -1
          val m = -1

          // ?s foaf:knows{-1, -1} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              Some(m),
              knowsUriFunc,
              true,
              config
            )

          result shouldBe a[Left[_, _]]
        }

        "from Some(1) to None path length (one or more)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          // ?s foaf:knows+ ?o
          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(1),
              None,
              knowsUriFunc,
              false,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(0) to None path length (zero or more)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          // ?s foaf:knows* ?o
          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(0),
              None,
              knowsUriFunc,
              false,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              null,
              "<http://example.org/Alice>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              null,
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              null,
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "\"Charles\"",
              null,
              "\"Charles\"",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              null,
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Erick>",
              null,
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(0) to Some(1) path length (zero or one)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          // ?s foaf:knows? ?o
          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(0),
              Some(1),
              knowsUriFunc,
              false,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              null,
              "<http://example.org/Alice>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              null,
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              null,
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "\"Charles\"",
              null,
              "\"Charles\"",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              null,
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Erick>",
              null,
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(3) to Some(3) path length (n == m) (exactly one)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 3
          val m = 3

          // ?s foaf:knows{3, 3} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              Some(m),
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from Some(2) to None path length" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 2

          // ?s foaf:knows{2,} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              Some(n),
              None,
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from None to Some(2) path length" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val n = 2

          // ?s foaf:knows{,2} ?o
          val result =
            FuncProperty.betweenNAndM(
              df,
              None,
              Some(n),
              knowsUriFunc,
              true,
              config
            )

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "\"Charles\"",
              null,
              "\"Charles\"",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              null,
              "<http://example.org/Alice>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              null,
              "<http://example.org/Bob>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              null,
              "<http://example.org/Charles>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              null,
              "<http://example.org/Daniel>",
              "\"\""
            ),
            Row(
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            ),
            Row(
              "<http://example.org/Erick>",
              null,
              "<http://example.org/Erick>",
              "\"\""
            )
          )
        }

        "from None to None path length (error)" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>",
              "\"\""
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\"",
              "\"\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>",
              "\"\""
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>",
              "\"\""
            )
          ).toTypedDF("s", "p", "o", "g").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          val result =
            FuncProperty.betweenNAndM(
              df,
              None,
              None,
              knowsUriFunc,
              true,
              config
            )

          result shouldBe a[Left[_, _]]
        }
      }
    }

    "NotOneOf function" should {

      "return expected values" when {

        "one uri" in {

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
          ).toTypedDF("s", "p", "o").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")

          // ?s !foaf:knows ?o
          val result =
            FuncProperty.notOneOf(df, List(knowsUriFunc))

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          )
        }

        "multiple uris" in {

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
          ).toTypedDF("s", "p", "o").@@[Untyped]

          lazy val knowsUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/knows>")
          lazy val fooUriFunc =
            FuncProperty.uri(df, "<http://xmlns.org/foaf/0.1/foo")

          // ?s !(foaf:knows|foaf:foo) ?o
          val result =
            FuncProperty.notOneOf(df, List(knowsUriFunc, fooUriFunc))

          result.right.get.unwrap.untype.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          )
        }
      }
    }
  }
}
