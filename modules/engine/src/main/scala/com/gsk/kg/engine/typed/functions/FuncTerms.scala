package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => _, _}

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.functions.Literals.nullLiteral
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine.typed.functions.TypedLiterals.isNumericLiteral_

object FuncTerms {

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    *
    * @param col
    * @return
    */
  def str(col: Column): Column =
    when(
      col.hasType(RdfType.Uri),
      RdfType.String(col.value)
    ).otherwise(col)

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    *
    * @param value
    * @return
    */
  def str(value: String): Column =
    str(DataFrameTyper.parse(lit(value)))

  /** Implementation of SparQL STRDT on Spark dataframes.
    * The STRDT function constructs a literal with lexical form and type as specified by the arguments.
    *
    * Examples:
    * STRDT("123", xsd:integer) -> "123"^^<http://www.w3.org/2001/XMLSchema#integer>
    * STRDT("iiii", <http://example/romanNumeral>) -> "iiii"^^<http://example/romanNumeral>
    *
    * @param col
    * @param uri
    * @return
    */
  def strdt(col: Column, uri: String): Column =
    DataFrameTyper.createRecord(col.value, DataFrameTyper.parse(lit(uri)).value)

  /** Implementation of SparQL STRLANG on Spark dataframes.
    * The STRLANG function constructs a literal with lexical form and language tag as specified by the arguments.
    *
    * Example:
    * STRLANG("chat", "en") -> "chat"@en
    *
    * @param col
    * @param tag
    * @return
    */
  def strlang(col: Column, tag: String): Column =
    DataFrameTyper.createRecord(col.value, col.`type`, lit(tag))

  /** The IRI function constructs an IRI by resolving the string
    * argument (see RFC 3986 and RFC 3987 or any later RFC that
    * supersedes RFC 3986 or RFC 3987). The IRI is resolved against
    * the base IRI of the query and must result in an absolute IRI.
    *
    * The URI function is a synonym for IRI.
    *
    * If the function is passed an IRI, it returns the IRI unchanged.
    *
    * Passing any RDF term other than a simple literal, xsd:string or
    * an IRI is an error.
    *
    * An implementation MAY normalize the IRI.
    *
    * =Examples=
    *
    * | Function call          | Result            |
    * |:-----------------------|:------------------|
    * | IRI("http://example/") | <http://example/> |
    * | IRI(<http://example/>) | <http://example/> |
    *
    * @param col
    * @return
    */
  def iri(col: Column): Column =
    when(
      col.hasType(RdfType.String) || col.hasType(RdfType.Uri),
      RdfType.Uri(col.value)
    ).otherwise(nullLiteral)

  /** synonym for [[FuncTerms.iri]]
    *
    * @param col
    * @return
    */
  def uri(col: Column): Column = iri(col)

  /** Implementation of SparQL DATATYPE on Spark dataframes
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-datatype]]
    * @param col
    * @return a Column containing the datatype IRI of for each literal
    */
  def datatype(col: Column): Column =
    RdfType.Uri(col.`type`)

  /** Implementation of SparQL LANG on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-lang]]
    * @param col
    * @return
    */
  def lang(col: Column): Column =
    when(
      col.lang.isNotNull,
      RdfType.String(col.lang)
    ).otherwise(RdfType.String(lit("")))

  /** Returns a column with 'true' or 'false' rows indicating whether a column has blank nodes
    *
    * @param col
    * @return
    */
  def isBlank(col: Column): Column =
    RdfType.Boolean(col.hasType(RdfType.Blank))

  /** Implementation of SparQL ISNUMERIC on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isNumeric]]
    * @param col
    * @return
    */
  def isNumeric(col: Column): Column =
    RdfType.Boolean(isNumericLiteral(col))

  /** Implementation of SparQL ISLITERAL on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isLiteral]]
    * @param col
    * @return
    */
  def isLiteral(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.contains("\"^^"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.endsWith("\""),
      lit(true)
    ).otherwise(lit(false))

  /** Returns UUID
    *
    * @return
    */
  def uuid: Column = {
    def uuidGen: () => String = () =>
      "urn:uuid:" + java.util.UUID.randomUUID().toString

    RdfType.Uri(udf(uuidGen).apply())
  }

  /** Return uuid
    *
    * @return
    */
  def strUuid: Column = {
    val u = uuid
    val startPos = lit("urn:uuid:".length + 1)
    val endPos = length(u.value)
    RdfType.String(u.value.substr(startPos, endPos))
  }
}
