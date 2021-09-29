package com.gsk.kg.engine.typed.functions

import com.gsk.kg.engine.{DataFrameTyper, RdfType}
import com.gsk.kg.engine.syntax._
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions._

object FuncHash {

  /** Implementation of SparQL MD5 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-md5]]
    * @param col
    * @return
    */
  def md5(col: Column): Column =
    RdfType.String(functions.md5(col.value))

  /** Implementation of SparQL MD5 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-md5]]
    * @param str
    * @return
    */
  def md5(str: String): Column =
    RdfType.String(functions.md5(DataFrameTyper.parse(lit(str))))

  /** Implementation of SparQL SHA1 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha1]]
    * @param col
    * @return
    */
  def sha1(col: Column): Column =
    RdfType.String(functions.sha1(col.value))

  /** Implementation of SparQL SHA1 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha1]]
    * @param str
    * @return
    */
  def sha1(str: String): Column =
    RdfType.String(functions.sha1(DataFrameTyper.parse(lit(str))))

  /** Implementation of SparQL SHA256 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha256]]
    * @param col
    * @return
    */
  def sha256(col: Column): Column = {
    val numBits = 256
    RdfType.String(sha2(col.value, numBits))
  }

  /** Implementation of SparQL SHA256 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha256]]
    * @param str
    * @return
    */
  def sha256(str: String): Column =
    sha256(DataFrameTyper.parse(lit(str)))

  /** Implementation of SparQL SHA384 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha384]]
    * @param col
    * @return
    */
  def sha384(col: Column): Column = {
    val numBits = 384
    RdfType.String(sha2(col.value, numBits))
  }

  /** Implementation of SparQL SHA384 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha384]]
    * @param str
    * @return
    */
  def sha384(str: String): Column =
    sha384(DataFrameTyper.parse(lit(str)))


  /** Implementation of SparQL SHA512 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha512]]
    * @param col
    * @return
    */
  def sha512(col: Column): Column = {
    val numBits = 512
    RdfType.String(sha2(col.value, numBits))
  }

  /** Implementation of SparQL SHA512 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha512]]
    * @param str
    * @return
    */
  def sha512(str: String): Column =
    sha512(DataFrameTyper.parse(lit(str)))
}
