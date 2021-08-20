package com.gsk.kg.benchmarks

import cats.data.NonEmptyList

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.functions.FuncArithmetics
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.rdf.RdfType
import com.gsk.kg.engine.rdf.TypedFuncs
import com.gsk.kg.engine.rdf.Typer

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class Benchmarks {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", "Benchmark")
    .set("spark.driver.host", "localhost")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.shuffle.partitions", "1")

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .config(sparkConf)
    .getOrCreate()

  import spark.implicits._

  implicit val sc = spark.sqlContext

  def isEven(n: Int) = n % 2 == 0

  val df        = (1 to 1000).map(x => if (isEven(x)) 0 else 1).toDF("col")
  val numericDf = (1 to 1000).zip((1 to 1000).map(_ / 10)).toDF("col1", "col2")
  val spoDf = List(
    (
      "<http://example.org/alice>",
      "<http://xmlns.com/foaf/0.1/name>",
      "Alice"
    ),
    (
      "<http://example.org/alice>",
      "<http://xmlns.com/foaf/0.1/age>",
      "21"
    ),
    (
      "<http://example.org/bob>",
      "<http://xmlns.com/foaf/0.1/name>",
      "Bob"
    ),
    (
      "<http://example.org/bob>",
      "<http://xmlns.com/foaf/0.1/age>",
      "15"
    )
  ).toDF("s", "p", "o")

  val spogDf = spoDf.withColumn("g", lit(""))

  val otherSpoDf = List(
    ("<http://asdf.com/pepe>", "<http://asdf.com/a>", "a"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/b>", "b"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/c>", "c"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/d>", "d"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/e>", "e")
  ).toDF("s", "p", "o")

  // @Benchmark def selectQueryUntyped(blackhole: Blackhole): Unit =
  //   Impl.selectQueryUntyped(otherSpoDf, blackhole)
  // @Benchmark def selectQueryTyped(blackhole: Blackhole): Unit =
  //   Impl.selectQueryTyped(otherSpoDf, blackhole)
  // @Benchmark def selectQueryWithFunctionsUntyped(blackhole: Blackhole): Unit =
  //   Impl.selectQueryWithFunctionsUntyped(spoDf, blackhole)
  // @Benchmark def selectQueryWithFunctionsTyped(blackhole: Blackhole): Unit =
  //   Impl.selectQueryWithFunctionsTyped(spoDf, blackhole)
  // @Benchmark def ifQueryUntyped(blackhole: Blackhole): Unit =
  //   Impl.ifQueryUntyped(spoDf, blackhole)
  // @Benchmark def ifQueryTyped(blackhole: Blackhole): Unit =
  //   Impl.ifQueryTyped(spoDf, blackhole)
  // @Benchmark def ifUntyped(blackhole: Blackhole): Unit =
  //   Impl.untypedIf(df, blackhole)
  // @Benchmark def ifTyped(blackhole: Blackhole): Unit =
  //   Impl.typedIf(df, blackhole)
  // @Benchmark def addUntyped(blackhole: Blackhole): Unit =
  //   Impl.untypedAdd(numericDf, blackhole)
  // @Benchmark def addTyped(blackhole: Blackhole): Unit =
  //   Impl.typedAdd(numericDf, blackhole)
  // @Benchmark def substractUntyped(blackhole: Blackhole): Unit =
  //   Impl.untypedSubstract(numericDf, blackhole)
  // @Benchmark def substractTyped(blackhole: Blackhole): Unit =
  //   Impl.typedSubstract(numericDf, blackhole)
  // @Benchmark def multiplyUntyped(blackhole: Blackhole): Unit =
  //   Impl.untypedMultiply(numericDf, blackhole)
  // @Benchmark def multiplyTyped(blackhole: Blackhole): Unit =
  //   Impl.typedMultiply(numericDf, blackhole)
  // @Benchmark def divideUntyped(blackhole: Blackhole): Unit =
  //   Impl.untypedDivide(numericDf, blackhole)
  // @Benchmark def divideTyped(blackhole: Blackhole): Unit =
  //   Impl.typedDivide(numericDf, blackhole)

  @Benchmark def queryyyTyped(blackhole: Blackhole): Unit =
    Impl.queryyyy(spogDf, blackhole)
}

object Impl {

  def untypedIf(df: DataFrame, blackhole: Blackhole): Unit = {
    val result = df
      .withColumn(
        "result",
        FuncForms.`if`(
          df("col"),
          lit("under 100"),
          lit("over 100")
        )
      )
      .collect

    blackhole.consume(result)
  }

  def typedIf(df: DataFrame, blackhole: Blackhole): Unit = {
    val typed = Typer.`type`(df)

    val result = Typer
      .untype(
        typed
          .withColumn(
            "result",
            TypedFuncs.`if`(
              col("col"),
              ifTrue =
                Typer.createRecord(lit("under 100"), RdfType.String.repr),
              ifFalse = Typer.createRecord(lit("over 100"), RdfType.String.repr)
            )
          )
      )
      .collect

    blackhole.consume(result)
  }

  def untypedAdd(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.add(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedSubstract(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.subtract(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedMultiply(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.multiply(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedDivide(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.divide(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def typedAdd(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.add(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedSubstract(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.subtract(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedMultiply(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.multiply(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedDivide(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.divide(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def ifQueryUntyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val query =
      """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?name ?cat
          |WHERE {
          | ?s foaf:name ?name .
          | ?s foaf:age ?age .
          | BIND(IF(?age < 18, "child", "adult") AS ?cat)
          |}
          |""".stripMargin

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    blackhole.consume(result)
  }

  def ifQueryTyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

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

    val result = Typer
      .untype(withNewColumn)
      .select("?name", "?cat")
      .collect

    blackhole.consume(result)
  }

  def selectQueryWithFunctionsUntyped(df: DataFrame, blackhole: Blackhole)(
      implicit sc: SQLContext
  ) = {
    val query = """
      PREFIX dm: <http://asdf.com/>

      SELECT DISTINCT ?b ?a WHERE {
        ?s dm:q ?c;
           dm:w ?b;
           dm:e ?d;
           dm:r "asdf";
           dm:t "qwer" .
        BIND(URI(CONCAT("http://example.com/",?d)) as ?e) .
        BIND(URI(CONCAT("http://example.com?asdf=",?c)) as ?a) .
      }
      """

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    blackhole.consume(result)
  }

  def selectQueryWithFunctionsTyped(df: DataFrame, blackhole: Blackhole)(
      implicit sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

    val first = typed
      .filter(col("s")("value") === "http://asdf.com/q")
      .select(col("s").as("?s"), col("o").as("?c"))

    val second = typed
      .filter(col("s")("value") === "http://asdf.com/w")
      .select(col("s").as("?s"), col("o").as("?b"))

    val third = typed
      .filter(col("s")("value") === "http://asdf.com/e")
      .select(col("s").as("?s"), col("o").as("?d"))

    val fourth = typed
      .filter(
        col("s")("value") === "http://asdf.com/r" && col("o")(
          "value"
        ) === "asdf"
      )
      .select(col("s").as("?s"))

    val fifth = typed
      .filter(
        col("s")("value") === "http://asdf.com/t" && col("o")(
          "value"
        ) === "asdf"
      )
      .select(col("s").as("?s"))

    val joined =
      first
        .join(second, "?s")
        .join(third, "?s")
        .join(fourth, "?s")
        .join(fifth, "?s")

    val withNewBindings =
      joined
        .withColumn(
          "?e",
          TypedFuncs.uri(
            TypedFuncs.concat(
              Typer.createRecord(lit("http://example.com"), RdfType.Uri.repr),
              NonEmptyList.of(col("?d"))
            )
          )
        )
        .withColumn(
          "?e",
          TypedFuncs.uri(
            TypedFuncs.concat(
              Typer.createRecord(
                lit("http://example.com?asdf="),
                RdfType.Uri.repr
              ),
              NonEmptyList.of(col("?c"))
            )
          )
        )
        .distinct()

    val result = withNewBindings.collect()

    blackhole.consume(result)
  }

  def selectQueryUntyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val query = """
      PREFIX dm: <http://asdf.com/>

      SELECT * WHERE {
        ?s dm:a ?a;
           dm:b ?b;
           dm:c ?c;
           dm:d ?d;
           dm:e ?e .
      }
      """

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    assert(!result.isEmpty)

    blackhole.consume(result)
  }

  def selectQueryTyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

    val first = typed
      .filter(col("s")("value") === "http://asdf.com/a")
      .select(col("s").as("?s"), col("o").as("?a"))

    val second = typed
      .filter(col("s")("value") === "http://asdf.com/b")
      .select(col("s").as("?s"), col("o").as("?b"))

    val third = typed
      .filter(col("s")("value") === "http://asdf.com/c")
      .select(col("s").as("?s"), col("o").as("?c"))

    val fourth = typed
      .filter(col("s")("value") === "http://asdf.com/d")
      .select(col("s").as("?s"), col("o").as("?d"))

    val fifth = typed
      .filter(col("s")("value") === "http://asdf.com/e")
      .select(col("s").as("?s"), col("o").as("?e"))

    val joined = first
      .join(second, "?s")
      .join(third, "?s")
      .join(fourth, "?s")
      .join(fifth, "?s")

    val result = joined.collect()

    assert(!result.isEmpty)

    blackhole.consume(result)
  }

  def queryyyy(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val first = Typer
      .`type`(df)

    val typed =
      first.filter(first("g")("value") === "https://pubmed.ncbi.nlm.nih.gov")

    val t1 = typed
      .filter(
        typed("p")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/dm/1.0/mappedTo" && typed("o")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/umls/CUI=C1421546"
      )
      .select(typed("s").as("?fle"))
    val t2 = typed
      .filter(
        typed("p")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/dm/1.0/mappedTo" && typed("o")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/umls/CUI=C1434936"
      )
      .select(typed("s").as("?fle"))
    val t3 = typed
      .filter(
        typed("p")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/dm/1.0/mappedTo" && typed("o")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/umls/CUI=C0764363"
      )
      .select(typed("s").as("?fle"))
    val t1_t2_t3_union = t1.union(t2).union(t3)
    val t4 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/entityLink"
      )
      .select(typed("s").as("?fde"), typed("o").as("?fle"))
    val t1_t2_t3_t4 = t1_t2_t3_union.join(t4, "?fle")
    val t5 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/hasSubject"
      )
      .select(typed("s").as("?pr"), typed("o").as("?fde"))
    val t6 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/hasObject"
      )
      .select(typed("s").as("?pr"), typed("o").as("?fde"))
    val t5_t6_union       = t5.union(t6)
    val t1_t2_t3_t4_t5_t6 = t1_t2_t3_t4.join(t5_t6_union, "?fde")
    val t7 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/contains"
      )
      .select(typed("s").as("?te"), typed("o").as("?pr"))
    val t1_t2_t3_t4_t5_t6_t7 = t1_t2_t3_t4_t5_t6.join(t7, "?pr")

    val t8 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/contains"
      )
      .select(typed("s").as("?te"), typed("o").as("?pred"))
    val t9 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/hasSubject"
      )
      .select(typed("s").as("?pred"), typed("o").as("?subjde"))

    val t8_t9    = t8.join(t9, "?pred")
    val t1_to_t9 = t1_t2_t3_t4_t5_t6_t7.join(t8_t9, "?te")

    val t10 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/entityLink"
      )
      .select(typed("s").as("?subjde"), typed("o").as("?subjle"))
    val t11 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/mappedTo"
      )
      .select(typed("s").as("?subjle"), typed("o").as("?subjconcept"))

    val t10_t11 = t10
      .join(t11, "?subjle")
      .withColumn(
        "?subjcui",
        TypedFuncs.strafter(
          col("?subjconcept"),
          "CUI="
        )
      )

    val t1_t11 = t1_to_t9.join(t10_t11, "?subjde")

    val t12 = typed
      .filter(
        typed("p")(
          "value"
        ) === "http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass"
      )
      .select(typed("s").as("?subjde"), typed("o").as("?subjpc"))
    val t13 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/predClass"
      )
      .select(typed("s").as("?subjpc"), typed("o").as("?subjClass"))
    val t14 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/indexStart"
      )
      .select(typed("s").as("?subjde"), typed("o").as("?subjStartIdx"))
    val t15 = typed
      .filter(
        typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/indexEnd"
      )
      .select(typed("s").as("?subjde"), typed("o").as("?subjEndIdx"))
    val t16 = typed
      .filter(typed("p")("value") === "http://gsk-kg.rdip.gsk.com/dm/1.0/text")
      .select(typed("s").as("?subjde"), typed("o").as("?subjText"))

    val t12_t16 = t12
      .join(t13, "?subjpc")
      .join(t14, "?subjde")
      .join(t15, "?subjde")
      .join(t16, "?subjde")

    val all = t1_t11
      .join(t12_t16, "?subjde")
      .select(
        "?te",
        "?pred",
        "?subjcui",
        "?subjClass",
        "?subjStartIdx",
        "?subjEndIdx",
        "?subjText"
      )

    val result = all.collect()

    blackhole.consume(result)
  }

  def q21(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val typed = Typer
      .`type`(df)

    def queryP(s: String, p: String, o: String) = 
      typed
        .filter(
          typed("p")("value") === p
        )
        .select(typed("s").as(s), typed("o").as(o))

    def queryPO(s: String, p: String, o: String) = 
      typed
        .filter(
          typed("p")("value") === p &&
          typed("o")("value") === o
        )
        .select(typed("s").as(s))

    val t1 = queryP("?pred", "dm:hasSubject", "?subjde")
    val t2 = queryP("?pred", "dm:hasObject", "?objde")
    val t3 = queryP("?objde", "dm:entityLink", "?objle")
    val t4 = queryPO("?objle", "dm:mappedTo", "<http://gsk-kg.rdip.gsk.com/umls/CUI=C1366587>")
    val t5 = queryP("?subjde", "dm:entityLink", "?subjle")
    val t6 = queryPO("?subjle", "dm:mappedTo", "<http://gsk-kg.rdip.gsk.com/umls/CUI=C1415615>")
    val t7 = queryP("?subjde", "dm:indexStart", "?subjStartIdx")
    val t8 = queryP("?subjde", "dm:indexEnd", "?subjEndIdx")
    val t9 = queryP("?subjde", "dm:text", "?subjText")
    val t10 = queryP("?objde", "dm:indexStart", "?objStartIdx")
    val t11  = queryP("?objde", "dm:indexStart", "?objEndIdx")
    val t12  = queryP("?objde", "dm:text", "?objText")
    val t13  = queryP("?te", "dm:contains", "?pred")
    val t14  = queryP("?te", "dm:text", "?text")
    val t15  = queryP("?ds", "dm:contains", "?te")
    val t16  = queryP("?doc", "dm:contains", "?ds")
    val t17  = queryP("?doc", "schema:title", "?title")
    val t18  = queryP("?doc", "dm:docSource", "?src")
    val t19  = queryP("?doc", "prism:doi", "?doi")
    val t20 = queryP("?doc", "dm:pubDateYear", "?year")
    val t21 = queryP("?doc", "dm:pubDateMonth", "?month")

    val result =
      t1
        .join(t2, "?pred")
        .join(t3, "?objde")
        .join(t4, "?objle")
        .join(t5, "?subjde")
        .join(t6, "?subjle")
        .join(t7, "?subjde")
        .join(t8, "?subjde")
        .join(t9, "?subjde")
        .join(t10, "?objde")
        .join(t11, "?objde")
        .join(t12, "?objde")
        .join(t13, "?pred")
        .join(t14, "?te")
        .join(t15, "?te")
        .join(t16, "?ds")
        .join(t17, "?doc")
        .join(t18, "?doc")
        .join(t19, "?doc")
        .join(t20, "?doc")
        .join(t21, "?doc")
        .collect()

    blackhole.consume(result)
  }
}
