package com.socrata.datacoordinator.truth.sql

import java.sql.{ResultSet, PreparedStatement}

trait SqlColumnCommonRep[Type] {
  /** The logical type of this column */
  val representedType: Type

  /** The "base name" from which physical column names are derived.  This must
    * be a legal SQL column name without quotes! */
  val base: String

  /** Physical SQL table columns used by this logical column; these must be
    * legal SQL column names without quotes */
  val physColumns: Array[String]

  /** Types of the physical SQL table columns used by this logical column.
    * @note This will have the same length as `physColumns`. */
  val sqlTypes: Array[String]

  /** Helper function to create physical column names for types with multiple physical columns. */
  protected def physCol(suffix: String) = "\"" + base + "$" + suffix + "\""

  val isPKableRep: Boolean = false
}

trait SqlColumnReadRep[Type, Value] extends SqlColumnCommonRep[Type] {
  /** Produces a column select list for this logical column.  It is
    * a set of prefixes and suffixes that are used to wrap the raw
    * column for extraction by "fromResultSet".  Specifically, it
    * wraps geometry types in ST_AsBinary.
    *
    * @note for each element, given some valid SQL expression "expr",
    *   the result of PREFIX + expr + SUFFIX must also be a valid
    *   sql expression.
    *
    * @note This will be the same length as `physColumns` */
  lazy val selectListTransforms: Array[(String, String)] = physColumns.map { _ => ("","") }

  /** The select list, transformed by `selectListTransforms`. */
  lazy val transformedSelectList: Array[String] =
    (physColumns, selectListTransforms).zipped.map { (col, wrapper) =>
      wrapper._1 + col + wrapper._2
    }

  /** Extract a value from the result set.  This will "use up" a number of
    * columns equal to `transformedSelectList.length`.
    */
  def fromResultSet(rs: ResultSet, start: Int): Value
  def asPKableRep: SqlPKableColumnReadRep[Type, Value] = sys.error("Not a PKable rep")
}

trait SqlOrderableColumnRep {  this: SqlColumnCommonRep[_] =>
  /** Produce an order specification for this column. */
  def orderBy(ascending: Boolean = true, nullsFirst: Option[Boolean] = None): String =
    simpleOrderBy(physColumns, ascending, nullsFirst)

  protected def simpleOrderBy(cols: Array[String], ascending: Boolean, nullsFirst: Option[Boolean]) = {
    val sb = new java.lang.StringBuilder
    def oneCol(col: String) {
      sb.append(col)
      sb.append(if(ascending) " ASC" else " DESC")
      nullsFirst.foreach { nf =>
        sb.append(if(nf) " NULLS FIRST" else " NULLS LAST")
      }
    }
    cols.foreach(oneCol)
    sb.toString
  }
}

trait SqlColumnWriteRep[Type, Value] extends SqlColumnCommonRep[Type] {
  /** @param sb The `StringBuilder` to which to add the data.
    * @param v The value to add; its type must be compatible with `representedType`.
    */
  def csvifyForInsert(sb: java.lang.StringBuilder, v: Value)

  /** Produce a set of prepared statement fragment which can be filled
    * in by `prepareInsert`.  It will be an array of strings, the same
    * length as `physColumns`, each element of which contains exactly
    * one "?" placeholder (possibly wrapped in extra functions).
    */
  lazy val insertPlaceholders: Array[String] = simpleTemplateForInsert
  final lazy val templateForInsert: String = insertPlaceholders.mkString(",")
  private def simpleTemplateForInsert = physColumns.map(_ => "?")

  /** Produces a series of functions, each of which fills in exactly
    * one placeholder in the given prepared statement; the length is
    * the same as `physColumns` and is called in that order.
    */
  val prepareInserts: Array[(PreparedStatement, Value, Int) => Unit]

  /** Fill in a template produced by `templateForInsert`.
    * @param start The position of the first "?" in the template
    * @return The first position after the last "?" in the template
    */
  final def prepareInsert(stmt: PreparedStatement, v: Value, start: Int): Int =
    prepareInserts.foldLeft(start) { (col, pi) =>
      pi(stmt, v, col)
      col + 1
    }

  /** Produce a prepared statement fragment which can be filled in by `prepareUpdate`.
    * It will contain one "?" for each value in `physColumns`, in that same order.
    */
  def templateForUpdate: String = simpleTemplateForUpdate
  private lazy val simpleTemplateForUpdate = physColumns.map(_ + "=?").mkString(",")

  /** Fill in a template produced by `templateForUpdate`.
    * @param start The position of the first "?" in the template
    * @return The first position after the last "?" in the template
    */
  def prepareUpdate(stmt: PreparedStatement, v: Value, start: Int): Int = prepareInsert(stmt, v, start)

  /** @return An estimate of the size of the data that would be generated by `csvifyForInsert` or `prepareInsert`.
    * @note This is only a ballpark estimate. */
  def estimateSize(v: Value): Int
}

trait SqlColumnRep[Type, Value] extends SqlColumnReadRep[Type, Value] with SqlColumnWriteRep[Type, Value] {
  override def asPKableRep: SqlPKableColumnRep[Type, Value] = sys.error("Not a PKable rep")
}

trait SqlPKableColumnReadRep[Type, Value] extends SqlColumnReadRep[Type, Value] with SqlOrderableColumnRep {

  def keyColumns: Array[String] = physColumns

  /** Generates sql equivalent to "column in (?, ...)" where there are `n` placeholders to be filled in by
    * `prepareMultiLookup`.
    * @param n The number of values to prepare slots for.
    * @note `n` must be greater than zero.
    */
  def templateForMultiLookup(n: Int): String

  /** Fill in one placeholder in a template created by `prepareMultiLookup`.
    * @return The position of the first prepared statement parameter after this placeholder.
    */
  def prepareMultiLookup(stmt: PreparedStatement, v: Value, start: Int): Int

  /** Generates a SQL expression equivalent to "`column in (literals...)`".
    * @param literals The `StringBuilder` to which to add the data.  Must be non-empty.
    *                 The individual values' types must be equal to (not merely compatible with!)
    *                 `representedType`.
    * @return An expression suitable for splicing into a SQL statement.
    */
  def sql_in(literals: Iterable[Value]): String

  /** Generates a SQL expression equivalent to "`count(column)`" */
  def count: String

  /** Generates SQL equivalent to "column = ?", where the placeholder can be filled
    * in by `prepareSingleLookup`
    */
  def templateForSingleLookup: String

  /** Fill in the placeholder in a template created by `prepareSingleLookup`.
    * @return The position of the first prepared statement parameter after this placeholder.
    */
  def prepareSingleLookup(stmt: PreparedStatement, v: Value, start: Int): Int

  /** Generates a SQL expression equivalent to "`column = literal`".
    * @param literal The `StringBuilder` to which to add the data.  The value's type
    *                must be equal to (not merely compatible with!) `representedType`.
    * @return An expression suitable for splicing into a SQL statement.
    */
  def sql_==(literal: Value): String

  /** Generates a SQL expression which can be used to create an index on this column suitable
    * for use in equality expressions. */
  def equalityIndexExpression: String

  override val isPKableRep = true

  override def asPKableRep: this.type = this
}

trait SqlPKableColumnRep[Type, Value] extends SqlColumnRep[Type, Value] with SqlPKableColumnReadRep[Type, Value]
