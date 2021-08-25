package com.gsk.kg.engine.relational

import cats.kernel.Eq
import cats.syntax.eq._
import higherkindness.droste.util.newtypes.@@
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/** The [[Relational]] typeclass captures the idea of a datatype [[A]] with
  * relational operations needed for compiling SparQL algebra
  *
  * =Laws=
  * {{{
  * Relational[A].union(Relational[A].empty, Relational[A].empty) <=> Relational[A].empty
  * }}}
  *
  * @see [[com.gsk.kg.engine.relational.RelationalLaws]]
  */
trait Relational[A] {
  def empty: A

  def isEmpty(left: A)(implicit A: Eq[A]): Boolean =
    left === empty

  def crossJoin(left: A, right: A): A
  def innerJoin(left: A, right: A, columns: Seq[String]): A
  def leftJoin(left: A, right: A, columns: Seq[String]): A
  def innerJoin(left: A, right: A, expression: Column): A
  def leftJoin(left: A, right: A, expression: Column): A
  def union(left: A, right: A): A
  def unionByName(left: A, right: A): A
  def minus(left: A, right: A): A
  def offset(df: A, offset: Long): A
  def limit(df: A, offset: Long): A
  def filter(df: A, condition: Column): A
  def distinct(df: A): A
  def drop(df: A, column: String): A
  def withColumn(df: A, name: String, expression: Column): A
  def withColumnRenamed(df: A, oldName: String, newName: String): A
  def columns(df: A): Array[String]
  def select(columns: Seq[Column]): A
}

object Relational extends RelationalInstances {
  sealed trait Untyped
  sealed trait Typed

  def apply[A](implicit A: Relational[A]): Relational[A] = A

  implicit class Ops[A](df: A)(implicit A: Relational[A]) {
    def isEmpty(implicit eq: Eq[A]): Boolean =
      Relational[A].isEmpty(df)
    def crossJoin(right: A): A =
      Relational[A].crossJoin(df, right)
    def innerJoin(right: A, columns: Seq[String]): A =
      Relational[A].innerJoin(df, right, columns)
    def leftJoin(right: A, columns: Seq[String]): A =
      Relational[A].leftJoin(df, right, columns)
    def innerJoin(right: A, expression: Column): A =
      Relational[A].innerJoin(df, right, expression)
    def leftJoin(right: A, expression: Column): A =
      Relational[A].leftJoin(df, right, expression)
    def union(right: A): A =
      Relational[A].union(df, right)
    def unionByName(right: A): A =
      Relational[A].unionByName(df, right)
    def minus(right: A): A =
      Relational[A].minus(df, right)
    def offset(offset: Long): A =
      Relational[A].offset(df, offset)
    def limit(limit: Long): A =
      Relational[A].limit(df, limit)
    def filter(condition: Column): A =
      Relational[A].filter(df, condition)
    def distinct(): A =
      Relational[A].distinct(df)
    def drop(column: String): A =
      Relational[A].drop(df, column)
    def withColumn(name: String, expression: Column): A =
      Relational[A].withColumn(df, name, expression)
    def withColumnRenamed(oldName: String, newName: String): A =
      Relational[A].withColumnRenamed(df, oldName, newName)
    def columns: Array[String] =
      Relational[A].columns(df)
    def select(columns: Seq[Column]): A =
      Relational[A].select(columns)
  }
}

trait RelationalInstances {

  import Relational._

  /** [[Relational]] implementation for untyped dataframes.  In our case,
    * untyped dataframes means dataframes with [[org.apache.spark.sql.types.StringType]]
    * columns, instead of [[org.apache.spark.sql.types.StructType]].
    *
    * @param df
    * @param sc
    * @return
    */
  implicit def untypedDataFrameRelational(df: DataFrame @@ Untyped)(implicit
      sc: SQLContext
  ): Relational[DataFrame @@ Untyped] = new Relational[DataFrame @@ Untyped] {
    def empty: DataFrame @@ Untyped =
      @@(sc.emptyDataFrame)

    def crossJoin(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped
    ): DataFrame @@ Untyped =
      @@(left.unwrap.crossJoin(right.unwrap))

    def innerJoin(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped,
        columns: Seq[String]
    ): DataFrame @@ Untyped =
      @@(left.unwrap.join(right.unwrap, columns, "inner"))

    def leftJoin(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped,
        columns: Seq[String]
    ): DataFrame @@ Untyped =
      @@(left.unwrap.join(right.unwrap, columns, "left"))

    def innerJoin(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped,
        expression: Column
    ): DataFrame @@ Untyped =
      @@(left.unwrap.join(right.unwrap, expression, "inner"))

    def leftJoin(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped,
        expression: Column
    ): DataFrame @@ Untyped =
      @@(left.unwrap.join(right.unwrap, expression, "left"))

    def union(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped
    ): DataFrame @@ Untyped =
      @@(left.unwrap.union(right.unwrap))

    def minus(
        left: DataFrame @@ Untyped,
        right: DataFrame @@ Untyped
    ): DataFrame @@ Untyped =
      @@(left.unwrap.exceptAll(right.unwrap))

    def offset(df: DataFrame @@ Untyped, offset: Long): DataFrame @@ Untyped =
      @@ {
        val rdd = df.unwrap.rdd.zipWithIndex
          .filter { case (_, idx) => idx >= offset }
          .map(_._1)
        sc.createDataFrame(rdd, df.unwrap.schema)
      }

    def limit(df: DataFrame @@ Untyped, l: Long): DataFrame @@ Untyped =
      @@ {
        df.unwrap.limit(l.toInt)
      }

    def filter(
        df: DataFrame @@ Untyped,
        condition: Column
    ): DataFrame @@ Untyped = @@ {
      df.unwrap.filter(condition)
    }

    def distinct(df: DataFrame @@ Untyped): DataFrame @@ Untyped = @@ {
      df.unwrap.distinct()
    }

    def drop(df: DataFrame @@ Untyped, column: String): DataFrame @@ Untyped =
      @@ {
        df.unwrap.drop(column)
      }

    def withColumn(
        df: DataFrame @@ Untyped,
        name: String,
        expression: Column
    ): DataFrame @@ Untyped = @@ {
      df.unwrap.withColumn(name, expression)
    }

    def withColumnRenamed(
        df: DataFrame @@ Untyped,
        oldName: String,
        newName: String
    ): DataFrame @@ Untyped = @@ {
      df.unwrap.withColumnRenamed(oldName, newName)
    }

    def columns(df: DataFrame @@ Untyped): Array[String] =
      df.unwrap.columns

    def select(columns: Seq[Column]): DataFrame @@ Untyped = @@ {
      df.unwrap.select(columns: _*)
    }
  }

}
