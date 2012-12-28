package com.socrata.datacoordinator
package truth.reader.sql

import java.sql.{DriverManager, Connection}

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{RowIdMap, DatasetContext}
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class SqlReaderTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.h2.Driver")
  }

  def datasetContext(schema: ColumnIdMap[TestColumnType]) = new DatasetContext[TestColumnType, TestColumnValue] {
    val typeContext = TestTypeContext

    val userSchema = schema

    val userPrimaryKeyColumn = if(schema.contains(new ColumnId(100L))) Some(new ColumnId(100L)) else None

    def userPrimaryKey(row: Row[TestColumnValue]) = row.get(userPrimaryKeyColumn.get)

    def systemId(row: Row[TestColumnValue]) = row.get(systemIdColumn).map(_.asInstanceOf[IdValue].value)

    def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumn)

    def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_ != systemIdColumn).toSet

    val systemSchema = ColumnIdMap[TestColumnType](systemIdColumn -> IdType)

    def systemIdColumn = new ColumnId(0L)

    val fullSchema = userSchema ++ systemSchema

    def makeIdMap[T]() = new RowIdMap[TestColumnValue, T] {
      val underlying = new scala.collection.mutable.HashMap[String, T]

      def s(x: TestColumnValue) = x.asInstanceOf[StringValue].value

      def put(x: TestColumnValue, v: T) {
        underlying += s(x) -> v
      }

      def apply(x: TestColumnValue) = underlying(s(x))

      def get(x: TestColumnValue) = underlying.get(s(x))

      def clear() { underlying.clear() }

      def contains(x: TestColumnValue) = underlying.contains(s(x))

      def isEmpty = underlying.isEmpty

      def size = underlying.size

      def foreach(f: (TestColumnValue, T) => Unit) {
        underlying.foreach { case (k,v) =>
          f(StringValue(k), v)
        }
      }

      def valuesIterator = underlying.valuesIterator
    }

    def mergeRows(base: Row[TestColumnValue], overlay: Row[TestColumnValue]) = sys.error("Shouldn't call this")
  }

  def managedConn = managed(DriverManager.getConnection("jdbc:h2:mem:"))

  def repSchemaBuilder(schema: ColumnIdMap[TestColumnType]): ColumnIdMap[SqlColumnReadRep[TestColumnType, TestColumnValue]] =
    schema.transform { (col, typ) =>
      typ match {
        case IdType => new IdRep(col)
        case NumberType => new NumberRep(col)
        case StringType => new StringRep(col)
      }
    }

  val tableName = "tab"

  def create(conn: Connection, schema: ColumnIdMap[TestColumnType]) {
    val sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (c_0 BIGINT NOT NULL PRIMARY KEY")
    for((k, v) <- schema.iterator) {
      sb.append(",").append("c_" + k.underlying).append(" ")
      val typ = v match {
        case StringType => "VARCHAR(255)"
        case NumberType => "BIGINT"
        case IdType => sys.error("shouldn't have IdType here")
      }
      sb.append(typ)
    }
    sb.append(")")

    using(conn.createStatement()) { stmt =>
      stmt.execute(sb.toString)
    }
  }

  def load(conn: Connection)(rows: Seq[(Long, TestColumnValue)]*) {
    using(conn.createStatement()) { stmt =>
      for(row <- rows) {
        val sb = new StringBuilder("INSERT INTO ").append(tableName).append(" (").append(row.iterator.map(c => "c_" + c._1).mkString(",")).append(") VALUES (")
        val vals = row.map(_._2).map {
          case IdValue(v) => v.underlying
          case NumberValue(v) => v
          case StringValue(v) => "'" + v.replaceAllLiterally("'","''") + "'"
          case NullValue => "null"
        }
        sb.append(vals.mkString(",")).append(")")
        stmt.execute(sb.toString)
      }
    }
  }

  def stdSidTable(conn: Connection) = {
    val schema = ColumnIdMap[TestColumnType](
      new ColumnId(1L) -> StringType,
      new ColumnId(2L) -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(new RowId(1)), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(new RowId(2)), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(new RowId(3)), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(new RowId(4)), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  def stdUidTable(conn: Connection) = {
    val schema = ColumnIdMap[TestColumnType](
      new ColumnId(100L) -> StringType,
      new ColumnId(1L) -> StringType,
      new ColumnId(2L) -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(new RowId(1)), 100L -> StringValue("alpha"), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(new RowId(2)), 100L -> StringValue("beta"), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(new RowId(3)), 100L -> StringValue("gamma"), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(new RowId(4)), 100L -> StringValue("delta"), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  test("Can read rows that exist from a table by system id") {
    val one = new ColumnId(1)
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(one), Iterator(4,3,2,1).map(new RowId(_))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(4) -> Some(ColumnIdMap(one -> NullValue)),
        new RowId(3) -> Some(ColumnIdMap(one -> StringValue("b"))),
        new RowId(2) -> Some(ColumnIdMap(one -> NullValue)),
        new RowId(1) -> Some(ColumnIdMap(one -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(new ColumnId(1L), new ColumnId(2L)), Iterator(new RowId(2))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(2) -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue, new ColumnId(2L) -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(new ColumnId(1L)), Iterator(4,99,98,1).map(new RowId(_))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(4) -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        new RowId(99) -> None,
        new RowId(98) -> None,
        new RowId(1) -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }

  test("Can read rows that exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L)), Iterator("delta", "gamma", "beta", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("gamma") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("b"))),
        StringValue("beta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("alpha") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L),new ColumnId(2L)), Iterator(StringValue("beta"))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("beta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue, new ColumnId(2L) -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L)), Iterator("delta", "psi", "omega", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("psi") -> None,
        StringValue("omega") -> None,
        StringValue("alpha") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }
}
