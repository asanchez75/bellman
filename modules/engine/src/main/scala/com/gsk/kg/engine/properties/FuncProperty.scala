package com.gsk.kg.engine.properties

import cats.Foldable
import cats.implicits.catsStdInstancesForList
import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.PropertyExpressionF.ColOrDf
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.functions.PathFrame._
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object FuncProperty {

  def alternative(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {
    val col = df("p")

    (pel, per) match {
      case (Left(l), Left(r)) =>
        Left(
          when(
            col.startsWith("\"") && col.endsWith("\""),
            FuncForms.equals(trim(col, "\""), l) ||
              FuncForms.equals(trim(col, "\""), r)
          ).otherwise(
            FuncForms.equals(col, l) ||
              FuncForms.equals(col, r)
          )
        ).asRight
      case _ =>
        EngineError
          .InvalidPropertyPathArguments(
            s"Invalid arguments on property path: seq, pel: ${pel.toString}, per: ${per.toString}," +
              s" both should be of type column"
          )
          .asLeft
    }
  }

  def seq(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {

    val resultL: Result[DataFrame] = (pel match {
      case Left(col)    => df.filter(df("p") === col)
      case Right(accDf) => accDf
    })
      .withColumnRenamed("s", "sl")
      .withColumnRenamed("p", "pl")
      .withColumnRenamed("o", "ol")
      .asRight[EngineError]

    val resultR: Result[DataFrame] = (per match {
      case Left(col) =>
        df.filter(df("p") === col)
      case Right(df) =>
        df
    }).withColumnRenamed("s", "sr")
      .withColumnRenamed("p", "pr")
      .withColumnRenamed("o", "or")
      .asRight[EngineError]

    for {
      l <- resultL
      r <- resultR
    } yield {
      Right(
        l
          .join(
            r,
            l("ol") <=> r("sr"),
            "inner"
          )
          .select(l("sl"), r("or"))
          .withColumnRenamed("sl", "s")
          .withColumnRenamed("or", "o")
      )
    }
  }

  def oneOrMore(df: DataFrame, e: ColOrDf)(implicit
      sc: SQLContext
  ): Result[ColOrDf] = {

    val onePaths = getOneLengthPaths(e match {
      case Right(predDf) => predDf
      case Left(predCol) => df.filter(predCol <=> df("p"))
    })

    val (m, pathsFrame) = constructPathFrame(onePaths, onePaths, limit = None)

    val paths = (1 to m).map(n => getNLengthPathTriples(pathsFrame, n)).toList

    val oneOrMorePaths = Foldable[List].fold(paths)

    toSPO(oneOrMorePaths)
      .asRight[Column]
      .asRight[EngineError]
  }

  def zeroOrMore(df: DataFrame, e: ColOrDf)(implicit
      sc: SQLContext
  ): Result[ColOrDf] = {

    val exprDf = e match {
      case Right(predDf) => predDf
      case Left(_)       => df
    }

    val zeroPaths = getZeroLengthPaths(df)

    oneOrMore(exprDf, e).flatMap {
      case Right(oneOrMorePaths) =>
        toSPO(oneOrMorePaths union zeroPaths)
          .asRight[Column]
          .asRight[EngineError]
      case Left(_) =>
        EngineError
          .InvalidPropertyPathArguments(
            "Error while processing zeroOrMore property path"
          )
          .asLeft[ColOrDf]
    }
  }

  def zeroOrOne(df: DataFrame, e: ColOrDf): Result[ColOrDf] = {

    val onePaths = getOneLengthPaths(e match {
      case Right(predDf) => predDf
      case Left(predCol) => df.filter(predCol <=> df("p"))
    })

    val zeroPaths = getZeroLengthPaths(df)

    toSPO(onePaths union zeroPaths)
      .asRight[Column]
      .asRight[EngineError]
  }

  def exactlyN(df: DataFrame, n: Int, e: ColOrDf): Result[ColOrDf] = {

    val onePaths = getOneLengthPaths(e match {
      case Right(predDf) => predDf
      case Left(predCol) => df.filter(predCol <=> df("p"))
    })

    val (_, pathsFrame) =
      constructPathFrame(onePaths, onePaths, limit = Some(n))

    toSPO(getNLengthPathTriples(pathsFrame, n))
      .asRight[Column]
      .asRight[EngineError]
  }

  def uri(s: String): ColOrDf =
    Left(lit(s))

}
