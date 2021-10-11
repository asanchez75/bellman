package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import com.gsk.kg.engine.typed.functions.TypedLiterals._
import org.apache.spark.sql.functions.round

object FuncArithmetics {

  /**
    * Add de value of two columns
    * @param l
    * @param r
    * @return
    */
  def add(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ + _)

  /**
    * Substract de value of two columns
    * @param l
    * @param r
    * @return
    */
  def subtract(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ - _)

  /**
    * Multiply de value of two columns
    * @param l
    * @param r
    * @return
    */
  def multiply(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ * _)

  /**
    * Divide de value of two columns
    * @param l
    * @param r
    * @return
    */
  def divide(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ / _)
}
