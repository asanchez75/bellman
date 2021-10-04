package com.gsk.kg.engine.properties

import cats.Foldable
import cats.implicits.catsStdInstancesForList
import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._

import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.functions.PathFrame._
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.relational.Relational.ops._
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object FuncProperty {

  def alternative(
      pel: DataFrame @@ Untyped,
      per: DataFrame @@ Untyped
  ): Result[DataFrame @@ Untyped] =
    pel.union(per).asRight

  def seq(
      pel: DataFrame @@ Untyped,
      per: DataFrame @@ Untyped,
      config: Config
  ): Result[DataFrame @@ Untyped] = {

    val (sLeft, pLeft, oLeft, gLeft)     = ("sl", "pl", "ol", "gl")
    val (sRight, pRight, oRight, gRight) = ("sr", "pr", "or", "gr")

    val resultL: Result[DataFrame @@ Untyped] = pel
      .withColumnRenamed(sCol, sLeft)
      .withColumnRenamed(pCol, pLeft)
      .withColumnRenamed(oCol, oLeft)
      .withColumnRenamed(gCol, gLeft)
      .asRight[EngineError]

    val resultR: Result[DataFrame @@ Untyped] = per
      .withColumnRenamed(sCol, sRight)
      .withColumnRenamed(pCol, pRight)
      .withColumnRenamed(oCol, oRight)
      .withColumnRenamed(gCol, gRight)
      .asRight[EngineError]

    for {
      l <- resultL
      r <- resultR
    } yield {
      val cnd = if (config.isDefaultGraphExclusive) {
        col(oLeft) <=> col(sRight) && col(gLeft) <=> col(gRight)
      } else {
        col(oLeft) <=> col(sRight)
      }

      l
        .innerJoin(r, cnd)
        .select(Seq(sLeft, oRight, pLeft, pRight, gLeft).map(col))
        .withColumnRenamed(sLeft, sCol)
        .withColumnRenamed(oRight, oCol)
        .withColumnRenamed(gLeft, gCol)
        .withColumn(pCol, lit(s"seq:${col(pLeft)}/${col(pRight)}"))
        .select(Seq(sCol, pCol, oCol, gCol).map(col))
    }
  }

  def betweenNAndM(
      df: DataFrame @@ Untyped,
      maybeN: Option[Int],
      maybeM: Option[Int],
      e: DataFrame @@ Untyped,
      duplicates: Boolean,
      config: Config
  )(implicit
      sc: SQLContext
  ): Result[DataFrame @@ Untyped] = {

    val checkArgs: Either[EngineError, (Int, Int)] = (maybeN, maybeM) match {
      case (Some(n), Some(m)) if n < 0 || m < 0 =>
        EngineError
          .InvalidPropertyPathArguments(
            "Arguments n and m can't be less than zero"
          )
          .asLeft
      case (Some(n), Some(m)) if n >= m =>
        (n, n).asRight // N >= M
      case (Some(n), Some(m)) =>
        (n, m).asRight // N < M
      case (Some(n), None) =>
        // TODO: Should be set a hard limit by configuration to avoid cycles???
        (n, 100).asRight // N or more
      case (None, Some(m)) =>
        (0, m).asRight // Between 0 and M
      case (None, None) =>
        EngineError
          .InvalidPropertyPathArguments(
            "Arguments n and m can't be both none"
          )
          .asLeft
    }

    checkArgs.flatMap { case (effectiveN, effectiveM) =>
      val (nonZeroPaths, zeroPaths) = (
        e.filter(col(pCol).isNotNull),
        e.filter(col(pCol).isNull)
      )

      val onePaths = getOneLengthPaths(nonZeroPaths)

      val (maxLength, pathsFrame) =
        constructPathFrame(onePaths, limit = Some(effectiveM), config)

      val effectiveRange =
        effectiveN to (if (effectiveM > maxLength) maxLength else effectiveM)

      val paths =
        effectiveRange.map { i =>
          getNLengthPathTriples(df, pathsFrame, i, config)
        }.toList

      val combinedPaths = Foldable[List].fold(paths)
      val betweenNAndMPaths = if (duplicates) { combinedPaths }
      else { combinedPaths.distinct }

      val merged = merge(toSPOG(betweenNAndMPaths), zeroPaths)
      val (mergedNoZeroPaths, mergedZeroPaths) = (
        merged.filter(col(pCol).isNotNull),
        merged.filter(col(pCol).isNull).distinct
      )

      merge(mergedNoZeroPaths, mergedZeroPaths)
        .asRight[EngineError]
    }
  }

  def notOneOf(df: DataFrame @@ Untyped, es: List[DataFrame @@ Untyped])(
      implicit sc: SQLContext
  ): Result[DataFrame @@ Untyped] = {

    val result = es
      .foldLeft(df) { case (acc, eDf) =>
        acc.except(eDf)
      }
      .distinct

    result.asRight
  }

  def uri(df: DataFrame @@ Untyped, s: String): DataFrame @@ Untyped =
    df.filter(col(pCol) <=> lit(s))

  def reverse(pe: DataFrame @@ Untyped): Result[DataFrame @@ Untyped] = {

    val (sTemp, oTemp) = ("sTemp", "oTemp")

    pe.withColumnRenamed(sCol, sTemp)
      .withColumnRenamed(oCol, oTemp)
      .withColumnRenamed(sTemp, oCol)
      .withColumnRenamed(oTemp, sCol)
      .select(Seq(sCol, pCol, oCol, gCol).map(col))
      .asRight
  }
}
