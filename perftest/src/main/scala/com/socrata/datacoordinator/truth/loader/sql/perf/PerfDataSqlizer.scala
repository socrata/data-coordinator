package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.postgresql.core.BaseConnection
import org.postgresql.copy.CopyManager
import com.socrata.datacoordinator.util.StringBuilderReader
import com.socrata.datacoordinator.truth.RowLogCodec

class PerfDataSqlizer(tableBase: String, user: String, val datasetContext: DatasetContext[PerfType, PerfValue], rowCodecFactory: () => RowLogCodec[PerfValue]) extends PerfSqlizer(datasetContext) with DataSqlizer[PerfType, PerfValue] {
  val userSqlized = PVText(user).sqlize
  val dataTableName = tableBase + "_data"
  val logTableName = tableBase + "_log"

  def typeContext = PerfTypeContext

  def sizeof(x: Long) = 8
  def sizeof(s: String) = s.length << 1
  def sizeof(bd: BigDecimal) = bd.precision
  def sizeofNull = 1

  def softMaxBatchSize = 2000000

  def sizerFrom(base: Int, t: PerfType): PerfValue => Int = t match {
    case PTId => {
      case PVId(v) => base+8
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
    case PTNumber => {
      case PVNumber(v) => base+v.precision
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
    case PTText => {
      case PVText(v) => base+v.length
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
  }
  def updateSizerForType(c: String, t: PerfType) = sizerFrom(c.length, t)
  def insertSizerForType(c: String, t: PerfType) = sizerFrom(0, t)

  val updateSizes = datasetContext.fullSchema.map { case (c, t) =>
    c -> updateSizerForType(c, t)
  }

  val insertSizes = datasetContext.fullSchema.map { case (c, t) =>
    c -> insertSizerForType(c, t)
  }

  def sizeofDelete = 8

  val baseUpdateSize = 50
  def sizeofUpdate(row: Row[PerfValue]) =
    row.foldLeft(baseUpdateSize) { (total, cv) =>
      val (c,v) = cv
      total + updateSizes(c)(v)
    }

  def sizeofInsert(row: Row[PerfValue]) =
    row.foldLeft(0) { (total, cv) =>
      val (c,v) = cv
      total + insertSizes(c)(v)
    }

  val mapToPhysical = Map.empty[String, String] ++ datasetContext.systemSchema.keys.map { k => k -> k.substring(1) } ++ datasetContext.userSchema.keys.map { k =>
    k -> ("u_" + k)
  }

  val logicalColumns = datasetContext.fullSchema.keys.toArray
  val physicalColumns = logicalColumns.map(mapToPhysical)

  val pkCol = mapToPhysical(datasetContext.primaryKeyColumn)

  val prepareSystemIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE id = ?"

  val bulkInsertStatement =
    "COPY " + dataTableName + " (" + physicalColumns.mkString(",") + ") from stdin with csv"

  def insertBatch(conn: Connection)(f: Inserter => Unit): Long = {
    val inserter = new InserterImpl
    f(inserter)
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
    copyManager.copyIn(bulkInsertStatement, inserter.reader)
  }

  def prepareForInsert(row: Row[PerfValue], systemId: Long) =
    row + (datasetContext.systemIdColumnName -> PVId(systemId))

  def prepareForUpdate(row: Row[PerfValue]) =
    row

  class InserterImpl extends Inserter {
    val sb = new java.lang.StringBuilder
    def insert(sid: Long, row: Row[PerfValue]) {
      val trueRow = prepareForInsert(row, sid)
      var didOne = false
      for(k <- logicalColumns) {
        if(didOne) sb.append(',')
        else didOne = true
        csvize(sb, k, trueRow.getOrElse(k, PVNull))
      }
      sb.append('\n')
    }

    def close() {}

    def reader: java.io.Reader = new StringBuilderReader(sb)
  }

  def csvize(sb: java.lang.StringBuilder, k: String, v: PerfValue) = {
    v match {
      case PVText(s) =>
        sb.append('"')
        var i = 0
        val limit = s.length
        while(i != limit) {
          val c = sb.charAt(i)
          sb.append(c)
          if(c == '"') sb.append('"')
          i += 1
        }
        sb.append('"')
      case PVNumber(n) =>
        sb.append(n)
      case PVId(n) =>
        sb.append(n)
      case PVNull =>
        // nothing
    }
  }

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long) = {
    stmt.setLong(1, sid)
    sizeof(sid)
  }

  def prepareSystemIdInsert(stmt: PreparedStatement, sid: Long, row: Row[PerfValue]) = {
    var totalSize = 0
    var i = 0
    val fullRow = prepareForInsert(row, sid)
    while(i != logicalColumns.length) {
      totalSize += setValue(stmt, i + 1, datasetContext.fullSchema(logicalColumns(i)), fullRow.getOrElse(logicalColumns(i), PVNull))
      i += 1
    }
    stmt.setLong(i + 1, sid)
    totalSize += sizeof(sid)

    totalSize
  }

  def sqlizeSystemIdUpdate(sid: Long, row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + prepareForUpdate(row).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE id = " + sid

  val prepareUserIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE " + pkCol + " = ?"

  def prepareUserIdDelete(stmt: PreparedStatement, id: PerfValue) = {
    setValue(stmt, 1, datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.get), id)
  }

  def setValue(stmt: PreparedStatement, col: Int, typ: PerfType, v: PerfValue): Int = {
    v match {
      case PVId(x) => stmt.setLong(col, x); sizeof(x)
      case PVNumber(x) => stmt.setBigDecimal(col, x.underlying); sizeof(x)
      case PVText(x) => stmt.setString(col, x); sizeof(x)
      case PVNull =>
        typ match {
          case PTId => stmt.setNull(col, java.sql.Types.INTEGER)
          case PTNumber => stmt.setNull(col, java.sql.Types.NUMERIC)
          case PTText => stmt.setNull(col, java.sql.Types.VARCHAR)
        }
        sizeofNull
    }
  }

  def sqlizeUserIdUpdate(row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + prepareForUpdate(row).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE " + pkCol + " = " + row(datasetContext.primaryKeyColumn).sqlize

  // txn log has (serial, row id, who did the update)
  val findCurrentVersion =
    "SELECT COALESCE(MAX(version), 0) FROM " + logTableName

  type LogAuxColumn = Array[Byte]

  val prepareLogRowsChangedStatement =
    "INSERT INTO " + logTableName + " (version, subversion, rows, who) VALUES (?, ?, ?," + userSqlized + ")"

  def prepareLogRowsChanged(stmt: PreparedStatement, version: Long, subversion: Long, rowsJson: LogAuxColumn) = {
    stmt.setLong(1, version)
    stmt.setLong(2, subversion)
    stmt.setBytes(3, rowsJson)
    sizeof(version) + sizeof(subversion) + rowsJson.length
  }

  def newRowAuxDataAccumulator(auxUser: (LogAuxColumn) => Unit) = new RowAuxDataAccumulator {
    var baos: java.io.ByteArrayOutputStream = _
    var out: java.io.DataOutputStream = _
    var rowCodec: RowLogCodec[PerfValue] = _
    var didOne: Boolean = _
    reset()

    def reset() {
      baos = new java.io.ByteArrayOutputStream
      out = new java.io.DataOutputStream(new org.xerial.snappy.SnappyOutputStream(baos))
      didOne = false
      rowCodec = rowCodecFactory()
      rowCodec.writeVersion(out)
    }

    def maybeFlush() {
      didOne=true
      if(baos.size > 128000) flush()
    }

    def flush() {
      if(didOne) {
        out.close()
        val bytes = baos.toByteArray
        reset()
        auxUser(bytes)
      }
    }

    def insert(sid: Long, row: Row[PerfValue]) {
      rowCodec.insert(out, sid, prepareForInsert(row, sid))
      maybeFlush()
    }

    def update(sid: Long, row: Row[PerfValue]) {
      rowCodec.update(out, sid, prepareForUpdate(row))
      maybeFlush()
    }

    def delete(systemID: Long) {
      rowCodec.delete(out, systemID)
      maybeFlush()
    }

    def finish() {
      flush()
    }
  }

  // This may batch the "ids" into multiple queries.  The queries
  def findSystemIds(ids: Iterator[PerfValue]) = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    ids.grouped(100).map { block =>
      block.map(_.sqlize).mkString("SELECT id AS sid, " + pkCol + " AS uid FROM " + dataTableName + " WHERE " + pkCol + " IN (", ",", ")")
    }
  }

  def extractIdPairs(rs: ResultSet) = {
    val typ = datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("extractIdPairs called without a user primary key")))

    val extractor = typ match {
      case PTNumber =>
        { () =>
          val l = rs.getBigDecimal("uid")
          if(l == null) PVNull
          else PVNumber(l)
        }
      case PTText =>
        { () =>
          val s = rs.getString("uid")
          if(s == null) PVNull
          else PVText(s)
        }
      case PTId =>
        { () =>
          val l = rs.getLong("uid")
          if(rs.wasNull) PVNull
          else PVId(l)
        }
    }

    def loop(): Stream[IdPair[PerfValue]] = {
      if(rs.next()) {
        val sid = rs.getLong("sid")
        val uid = extractor()
        IdPair(sid, uid) #:: loop()
      } else {
        Stream.empty
      }
    }
    loop().iterator
  }
}
