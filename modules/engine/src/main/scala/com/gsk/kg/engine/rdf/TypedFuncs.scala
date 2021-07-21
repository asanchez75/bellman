package com.gsk.kg.engine.rdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object TypedFuncs {

  def `if`(cnd: Column, ifTrue: Column, ifFalse: Column): Column = {
    val ebv = effectiveBooleanValue(cnd)
    when(
      ebv.isNotNull,
      when(
        ebv,
        ifTrue
      ).otherwise(ifFalse)
    ).otherwise(lit(null)) // scalastyle:off
  }

  def effectiveBooleanValue(col: Column): Column = {
    lazy val ifNumeric = when(
      col("value").cast("double").isNull,
      lit(false)
    ).otherwise {
      val double = col("value").cast("double")
      when(
        isnan(double) || double === lit(0.0),
        lit(false)
      ).otherwise(lit(true))
    }

    lazy val ifStringLit = when(
      col("value") === lit(""),
      lit(false)
    ).otherwise(lit(true))

    def isBoolean(col: Column): Column =
      col("type") === RdfType.Boolean.repr

    def isString(col: Column): Column =
      col("type") === RdfType.String.repr

    def isNumeric(col: Column): Column =
      col("type") === RdfType.Double.repr ||
        col("type") === RdfType.Int.repr ||
        col("type") === RdfType.Decimal.repr

    when(
      isBoolean(col),
      col("value")
    ).when(
      isNumeric(col),
      ifNumeric
    ).when(
      isString(col),
      ifStringLit
    ).otherwise(lit(null)) // scalastyle:off
  }

}
