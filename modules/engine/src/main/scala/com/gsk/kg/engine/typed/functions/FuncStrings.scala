package com.gsk.kg.engine.typed.functions

import cats.data.NonEmptyList
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine._
import com.gsk.kg.engine.typed.functions.FuncStrings.StringFuncUtils._
import com.gsk.kg.engine.functions.Literals._
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => cc, _}

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.regex.Pattern

object FuncStrings {

  /** Implementation of SparQL STRLEN on Spark dataframes.
    * Counts string number of characters
    *
    * strlen("chat") -> 4
    * strlen("chat"@en) -> 4
    * strlen("chat"^^xsd:string) -> 4
    *
    * @param col
    * @return
    */
  def strlen(col: Column): Column =
    RdfType.Int(length(col.value))

  /** Implementation of SparQL SUBSTR on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-substr]]
    * @param col
    * @param pos
    * @param len
    * @return
    */
  def substr(col: Column, pos: Int, len: Option[Int]): Column =
    RdfType.String {
      len match {
        case Some(l) => col.value.substr(pos, l)
        case None => col.value.substr(lit(pos), length(col.value) - pos + 1)
      }
    }

  /** Implementation of SparQL UCASE on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-ucase]]
    * @param col
    * @return
    */
  def ucase(col: Column): Column =
    DataFrameTyper.createRecord(upper(col.value), RdfType.String.repr, col.lang)

  /** Implementation of SparQL LCASE on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-lcase]]
    * @param col
    * @return
    */
  def lcase(col: Column): Column =
    DataFrameTyper.createRecord(lower(col.value), RdfType.String.repr, col.lang)

  /** Implementation of SparQL STRSTARTS on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strstarts]]
    * @param col
    * @param str
    * @return
    */
  def strstarts(col: Column, str: String): Column =
    RdfType.Boolean(col.value.startsWith(extractStringLiteral(str)))

  /** Implementation of SparQL STRENDS on Spark dataframes.
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strends]]
    * @param col
    * @param str
    * @return
    */
  def strends(col: Column, str: String): Column =
    RdfType.Boolean(col.value.endsWith(extractStringLiteral(str)))

  /** Implementation of SparQL STRBEFORE on Spark dataframes.
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strbefore]]
    * @param col
    * @param str
    * @return
    */
  def strbefore(col: Column, str: String): Column = {

    def getLeftOrEmpty(c: Column, s: String): Column =
      when(substring_index(c, s, 1) === c, lit(""))
        .otherwise(substring_index(c, s, 1))

    DataFrameTyper.createRecord(
      getLeftOrEmpty(col.value, str),
      col.`type`,
      col.lang
    )
  }

  /** Implementation of SparQL STRAFTER on Spark dataframes.
    *
    * =Examples=
    *
    * | Function call                  | Result            |
    * |:-------------------------------|:------------------|
    * | strafter("abc","b")            | "c"               |
    * | strafter("abc"@en,"ab")        | "c"@en            |
    * | strafter("abc"@en,"b"@cy)      | error             |
    * | strafter("abc"^^xsd:string,"") | "abc"^^xsd:string |
    * | strafter("abc","xyz")          | ""                |
    * | strafter("abc"@en, "z"@en)     | ""                |
    * | strafter("abc"@en, "z")        | ""                |
    * | strafter("abc"@en, ""@en)      | "abc"@en          |
    * | strafter("abc"@en, "")         | "abc"@en          |
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strafter]]
    * @param col
    * @param str
    * @return
    */
  def strafter(col: Column, str: String): Column = {

    def getLeftOrEmpty(c: Column, s: String): Column =
      when(substring_index(c, s, -1) === c, lit(""))
        .otherwise(substring_index(c, s, -1))

    if (isEmptyPattern(str)) {
      col
    } else {
      DataFrameTyper.createRecord(
        getLeftOrEmpty(col.value, str),
        col.`type`,
        col.lang
      )
    }
  }

  /** Implementation of SparQL ENCODE_FOR_URI on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-encode]]
    * @param str
    * @return
    */
  def encodeForURI(str: String): Column =
    RdfType.String(lit(encodeUri(extractStringLiteral(str))))

  /** Implementation of SparQL ENCODE_FOR_URI on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-encode]]
    * @param col
    * @return
    */
  def encodeForURI(col: Column): Column = {
    val efu = udf((str: String) => encodeUri(str))
    RdfType.String(efu(col.value))
  }

  /** Concatenate two [[Column]] into a new one
    *
    * @param a
    * @param b
    * @return
    */
  def concat(appendTo: Column, append: NonEmptyList[Column]): Column = {
    val lvalue = appendTo.value
    val ltag = appendTo.lang

    val concatValues = append.toList.foldLeft(lvalue) { case (acc, elem) =>
      cc(acc, elem.value)
    }

    when(
      allArgsAreSameTypeAndLang(appendTo, append.toList),
      DataFrameTyper.createRecord(concatValues, RdfType.String.repr, appendTo.lang)
    ).otherwise(
      RdfType.String(concatValues)
    )
  }

  /** Implementation of SparQL LANGMATCHES on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-langMatches]]
    * @param col
    * @return
    */
  def langMatches(col: Column, range: String): Column = {

    def hasMatchingLangTag(tag: String, range: String): Boolean = {
      import scala.collection.JavaConverters._
      !Locale
        .filter(
          Locale.LanguageRange.parse(range),
          List(Locale.forLanguageTag(tag)).asJava
        )
        .isEmpty
    }

    val langMatch =
      udf((tag: String, range: String) => hasMatchingLangTag(tag, range))

    when(col.value === lit("") && range == "*", RdfType.Boolean.False)
      .otherwise(RdfType.Boolean(langMatch(col.value, lit(range))))
  }

  /** Implementation of SparQL REGEX on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-regex]]
    * @param col
    * @param pattern
    * @param flags
    * @return
    */
  def regex(col: Column, pattern: String, flags: String): Column =
    RdfType.Boolean(col.value.rlike(s"(?$flags)$pattern"))

  /** Implementation of SparQL REPLACE on Spark dataframes.
    *
    * =Examples=
    *
    * | Function call                              | Result                     |
    * |:-------------------------------------------|:---------------------------|
    * | replace("abracadabra", "bra", "*")         | "a*cada*"                  |
    * | replace("abracadabra", "a.*a", "*")        | "*"                        |
    * | replace("abracadabra", "a.*?a", "*")       | "*c*bra"                   |
    * | replace("abracadabra", "a", "")            | "brcdbr"                   |
    * | replace("abracadabra", "a(.)", "a$1$1")    | "abbraccaddabbra"          |
    * | replace("abracadabra", ".*?", "$1")        | error (zero length string) |
    * | replace("AAAA", "A+", "b")                 | "b"                        |
    * | replace("AAAA", "A+?", "b")                | "bbbb"                     |
    * | replace("darted", "^(.*?)d(.*)$", "$1c$2") | "carted"                   |
    *
    * @see https://www.w3.org/TR/sparql11-query/#func-replace
    * @see https://www.w3.org/TR/xpath-functions/#func-replace
    * @param col
    * @param pattern
    * @param by
    * @param flags
    * @return
    */
  def replace(col: Column, pattern: String, by: String, flags: String): Column =
    regexp_replace(col, s"(?$flags)$pattern", by)

  object StringFuncUtils {

    def unfold(arg: Column): (Column, Column) = {
      val getValue = when(
        RdfFormatter.isLocalizedString(arg), {
          val l = LocalizedLiteral(arg)
          trim(l.value, "\"")
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(arg), {
          val l = TypedLiteral(arg)
          trim(l.value, "\"")
        }
      ).otherwise(
        trim(arg, "\"")
      )

      val getTag = when(
        RdfFormatter.isLocalizedString(arg), {
          val l = LocalizedLiteral(arg)
          l.tag
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(arg), {
          val l = TypedLiteral(arg)
          l.tag
        }
      ).otherwise(
        lit("")
      )

      (getValue, getTag)
    }

    def allArgsAreSameTypeAndLang(
                                   arg1: Column,
                                   args: List[Column]
                                 ): Column = {
      when(
        arg1.lang.isNotNull, {
          args.foldLeft(lit(true)) { case (acc, elem) =>
            acc && when(
              elem.lang.isNotNull,
              elem.lang === arg1.lang
            ).otherwise(lit(false))
          }
        }
      ).otherwise {
        args.foldLeft(lit(true)) { case (acc, elem) =>
          acc && elem.`type` === arg1.`type`
        }
      }
    }

    def isLocalizedLocalizedArgs(arg1: Column, arg2: String): Column =
      RdfFormatter.isLocalizedString(arg1) && RdfFormatter.isLocalizedString(
        lit(arg2)
      )

    def isTypedTypedArgs(arg1: Column, arg2: String): Column =
      RdfFormatter.isDatatypeLiteral(arg1) && RdfFormatter.isDatatypeLiteral(
        lit(arg2)
      )

    def isTypedPlainArgs(arg1: Column): Column =
      RdfFormatter.isDatatypeLiteral(arg1)

    def isLocalizedPlainArgs(arg1: Column): Column =
      RdfFormatter.isLocalizedString(arg1)

    def strFuncArgsLocalizedLocalized(
                                       col: Column,
                                       str: String,
                                       localizedFormat: String
                                     )(
                                       f: (Column, String) => Column
                                     ): Column = {
      val left = LocalizedLiteral(col)
      val right = LocalizedLiteral(str)
      when(
        left.tag =!= right.tag,
        nullLiteral
      ).otherwise(
        LocalizedLiteral.formatLocalized(left, str, localizedFormat)(f)
      )
    }

    def strFuncArgsLocalizedPlain(
                                   col: Column,
                                   str: String,
                                   localizedFormat: String
                                 )(
                                   f: (Column, String) => Column
                                 ): Column = {
      val left = LocalizedLiteral(col)
      LocalizedLiteral.formatLocalized(left, str, localizedFormat)(f)
    }

    def strFuncArgsTypedTyped(col: Column, str: String, typedFormat: String)(
      f: (Column, String) => Column
    ): Column = {
      val left = TypedLiteral(col)
      val right = TypedLiteral(str)
      when(
        left.tag =!= right.tag,
        nullLiteral
      ).otherwise(
        TypedLiteral.formatTyped(left, str, typedFormat)(f)
      )
    }

    def strFuncArgsTypedPlain(col: Column, str: String, typedFormat: String)(
      f: (Column, String) => Column
    ): Column = {
      val left = TypedLiteral(col)
      TypedLiteral.formatTyped(left, str, typedFormat)(f)
    }
  }

  private def encodeUri(str: String): String =
    str.map {
      case c if Pattern.matches("[A-Za-z0-9~._-]", c.toString) => c.toString
      case c =>
        val hex =
          Hex.encodeHexString(c.toString.getBytes(StandardCharsets.UTF_8))
        if (hex.length > 2) {
          hex.grouped(hex.length / 2).map("%" + _.toUpperCase).mkString
        } else {
          "%" + hex.toUpperCase
        }
    }.mkString

  private def applyRdfFormat(col: Column)(f: Column => Column): Column = {
    when(
      col.contains("\"@"),
      formatRdfString(col, "@")(f)
    ).when(
      col.contains("\"^^"),
      formatRdfString(col, "^^")(f)
    ).otherwise(f(trim(col, "\"")))
  }

  private def formatRdfString(col: Column, sep: String)(
    f: Column => Column
  ): Column = {
    format_string(
      "%s",
      cc(
        cc(
          cc(
            lit("\""),
            f(trim(substring_index(col, "\"" + sep, 1), "\""))
          ),
          lit("\"")
        ),
        cc(lit(sep), substring_index(col, "\"" + sep, -1))
      )
    )
  }

  private def isEmptyPattern(pattern: String): Boolean = {
    if (pattern.isEmpty) {
      true
    } else if (pattern.contains("@")) {
      val left = pattern.split("@").head.replace("\"", "")
      if (left.isEmpty) {
        true
      } else {
        false
      }
    } else if (pattern.contains("^^")) {
      val left = pattern.split("\\^\\^").head.replace("\"", "")
      if (left.isEmpty) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}
