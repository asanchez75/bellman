package com.gsk.kg.engine.properties

import cats.Foldable
import cats.implicits.catsStdInstancesForList
import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._
import higherkindness.droste.util.newtypes.@@
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.gsk.kg.engine.PropertyExpressionF.ColOrDf
import com.gsk.kg.engine.functions.PathFrame._
import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.relational.Relational.ops._
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object FuncProperty {

  def alternative(
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {
    (pel, per) match {
      case (Left(l), Left(r)) =>
        (l || r).asLeft.asRight
      case (Right(lDf), Right(rDf)) =>
        lDf.union(rDf).asRight.asRight
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
      df: DataFrame @@ Untyped,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {

    val resultL: Result[DataFrame @@ Untyped] = (pel match {
      case Left(col)    => df.filter(col(pCol) === col)
      case Right(accDf) => accDf
    })
      .withColumnRenamed(sCol, "sl")
      .withColumnRenamed(pCol, "pl")
      .withColumnRenamed(oCol, "ol")
      .withColumnRenamed(gCol, "gl")
      .asRight[EngineError]

    val resultR: Result[DataFrame @@ Untyped] = (per match {
      case Left(col) =>
        df.filter(col(pCol) === col)
      case Right(df) =>
        df
    }).withColumnRenamed(sCol, "sr")
      .withColumnRenamed(pCol, "pr")
      .withColumnRenamed(oCol, "or")
      .withColumnRenamed(gCol, "gr")
      .asRight[EngineError]

    for {
      l <- resultL
      r <- resultR
    } yield {
      val joinResult = l
        .innerJoin(
          r,
          (col("ol") <=> col("sr") &&
            col("gl") <=> col("gr"))
        )

      val result = joinResult
        .select(Seq(col("sl"), col("or"), col("pl"), col("pr"), col("gl")))
        .withColumnRenamed("sl", sCol)
        .withColumnRenamed("or", oCol)
        .withColumnRenamed("gl", gCol)
        .withColumn(pCol, lit(s"seq:${col("pl")}/${col("pr")}"))
        .select(Seq(sCol, pCol, oCol, gCol).map(col))

      result.asRight
    }
  }

  def betweenNAndM(
      df: DataFrame @@ Untyped,
      maybeN: Option[Int],
      maybeM: Option[Int],
      e: ColOrDf,
      duplicates: Boolean
  )(implicit
      sc: SQLContext
  ): Result[ColOrDf] = {

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
      val (nonZeroPaths, zeroPaths) = e match {
        case Right(predDf) =>
          (
            predDf.filter(col(pCol).isNotNull),
            predDf.filter(col(pCol).isNull)
          )
        case Left(predCol) =>
          (df.filter(predCol), df.filter(predCol && col(pCol).isNull))
      }

      val onePaths = getOneLengthPaths(nonZeroPaths)

      val (maxLength, pathsFrame) =
        constructPathFrame(onePaths, limit = Some(effectiveM))

      val effectiveRange =
        effectiveN to (if (effectiveM > maxLength) maxLength else effectiveM)

      val paths =
        effectiveRange.map { i =>
          getNLengthPathTriples(df, pathsFrame, i)
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
        .asRight[Column]
        .asRight[EngineError]
    }
  }

  def notOneOf(df: DataFrame @@ Untyped, es: List[ColOrDf])(implicit
      sc: SQLContext
  ): Result[ColOrDf] = {

    val emptyDfWithSchema =
      Relational[DataFrame @@ Untyped].emptyWithSchema(df.schema)

    val result = es
      .map(_.right.get)
      .foldLeft(emptyDfWithSchema) { case (acc, eDf) =>
        acc union {
          val step: DataFrame @@ Untyped = @@(
            df.unwrap
              .except(eDf.unwrap)
              .toDF()
          )

          println("step")
          step.show(false)

          step
        }
      }
      .distinct

    println("result")
    result.show(false)

    result.asRight.asRight
//
//    val resolveColCnd: ColOrDf => Result[Column] = { expr: ColOrDf =>
//      expr match {
//        case Right(exprDf) =>
//          val join = df.leftOuter(exprDf, Seq(sCol, pCol, oCol, gCol))
//
//          println("not one of:")
//          join.show(false)
//
//          join.asRight
//        case Left(predCol) =>
//          not(predCol).asRight
//      }
//    }
//
//    val zero = lit(true).asRight[EngineError]
//
//    es.foldLeft(zero) { case (accOrError, colOrDf) =>
//      for {
//        acc    <- accOrError
//        colCnd <- resolveColCnd(colOrDf)
//      } yield acc && colCnd
//    }.map { cnd =>
//      df.filter(cnd).asRight
//    }
  }

  def uri(df: DataFrame @@ Untyped, s: String): ColOrDf =
    df.filter(col(pCol) <=> lit(s)).asRight

  def reverse(pe: ColOrDf): Result[ColOrDf] = {

    def reverseDf(df: DataFrame @@ Untyped): DataFrame @@ Untyped = {
      df.withColumnRenamed(sCol, "sTemp")
        .withColumnRenamed(oCol, "oTemp")
        .withColumnRenamed("sTemp", oCol)
        .withColumnRenamed("oTemp", sCol)
        .select(Seq(sCol, pCol, oCol, gCol).map(col))
    }

    pe match {
      case Right(df) =>
        reverseDf(df).asRight.asRight
      case Left(col) => ???
    }
  }
}
