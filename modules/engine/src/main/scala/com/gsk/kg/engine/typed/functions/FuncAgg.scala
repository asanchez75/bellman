package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax._

object FuncAgg {

  /** Sample is a set function which returns an arbitrary value from
    * the Multiset[A] passed to it.
    *
    * Implemented using [[org.apache.spark.sql.functions.first]].
    *
    * @param col
    * @return
    */
  def sample(col: Column): Column =
    first(col, true)

  /** This functions count the number of elements in a group
    *
    * @param col
    * @return
    */
  def countAgg(col: Column): Column =
    count(col)

  /** This function calculates the average on a numeric type group
    *
    * @param col
    * @return
    */
  def avgAgg(col: Column): Column =
    RdfType.Double(avg(col.value))

  /** This function calculates the sum on a numeric type group
    *
    * @param col
    * @return
    */
  def sumAgg(col: Column): Column =
    RdfType.Double(sum(col.value))

  /** This function calculates the minimum of a group when numeric or other literals like strings
    *
    * @param col
    * @return
    */
  def minAgg(col: Column): Column =
    DataFrameTyper.createRecord(min(col.value), first(col.`type`))

  /** This funciton calculates the maximum of a group when numeric or other literals like strings
    *
    * @param col
    * @return
    */
  def maxAgg(col: Column): Column =
    DataFrameTyper.createRecord(max(col.value), first(col.`type`))

  // TODO: Implement group-concat https://github.com/gsk-aiops/bellman/issues/202
  def groupConcat(col: Column, separator: String): Column =
    ???
}
