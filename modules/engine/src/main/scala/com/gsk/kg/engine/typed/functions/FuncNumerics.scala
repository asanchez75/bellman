package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{abs => sAbs}
import org.apache.spark.sql.functions.{ceil => sCeil}
import org.apache.spark.sql.functions.{floor => sFloor}
import org.apache.spark.sql.functions.{rand => sRand}
import org.apache.spark.sql.functions.{round => sRound}
import org.apache.spark.sql.types.IntegerType

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine.typed.functions.TypedLiterals._

object FuncNumerics {

  /** Returns the absolute value of arg. An error is raised if arg is not a numeric value.
    *
    * @param col
    * @return
    */
  def abs(col: Column): Column = apply(sAbs, col)

  /** Returns the number with no fractional part that is closest to the argument.
    * If there are two such numbers, then the one that is closest to positive infinity
    * is returned. An error is raised if arg is not a numeric value.
    *
    * @param col
    * @return
    */
  def round(col: Column): Column = apply(sRound, col, c => RdfType.Int(c.cast(IntegerType)))

  /** Returns the smallest (closest to negative infinity) number with no fractional part
    * that is not less than the value of arg. An error is raised if arg is not a numeric value.
    *
    * @param col
    * @return
    */
  def ceil(col: Column): Column = apply(sCeil, col, c => RdfType.Int(c.cast(IntegerType)))

  /** Returns the largest (closest to positive infinity) number with no fractional part that is not greater
    * than the value of arg. An error is raised if arg is not a numeric value.
    *
    * @param col
    * @return
    */
  def floor(col: Column): Column = apply(sFloor, col, c => RdfType.Int(c.cast(IntegerType)))

  /** Returns a pseudo-random number between 0 (inclusive) and 1.0e0 (exclusive). Different numbers can be
    * produced every time this function is invoked. Numbers should be produced with approximately equal probability.
    *
    * @return
    */
  def rand: Column = RdfType.Double(sRand())

  /** Apply function f over column col detecting malformed data
    * e.g.
    * case col = 10                      ==> f(10)
    * case col = "\"2\"^^xsd:int"        ==> f(2)
    * case col = "\"-0.3\"^^xsd:decimal" ==> f(-0.3)
    * case col = "\"-10.5\"^^xsd:string" ==> null, this value is incorrect
    * case col = "\"2.3\"^^xsd:int"      ==> null, this value is incorrect
    *
    * @param f
    * @param col
    * @return
    */
  private def apply(f: Column => Column, col: Column, `type`: Column => Column = DataFrameTyper.parse): Column =
    when(
      isNumericLiteral(col),
      `type`(f(col.value))
    ).otherwise(nullLiteral)
}
