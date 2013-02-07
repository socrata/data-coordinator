package com.socrata.datacoordinator.backup

import java.io.{InputStreamReader, InputStream, DataInputStream, DataOutputStream}
import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.loader.Delogger.LogEvent
import com.rojoma.json.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.RowLogCodec

trait Codec[T] {
  def encode(target: DataOutputStream, data: T)
  def decode(input: DataInputStream): T
}

class LogDataCodec[CV](rowLogCodecFactory: () => RowLogCodec[CV]) extends Codec[Delogger.LogEvent[CV]] {
  def encode(dos: DataOutputStream, event: LogEvent[CV]) {
    dos.write(event.productPrefix.getBytes)
    dos.write(0)
    event match {
      case Delogger.RowDataUpdated(bytes) =>
        dos.writeInt(bytes.length)
        dos.write(bytes)
      case Delogger.RowIdCounterUpdated(rid) =>
        dos.writeLong(rid.underlying)
      case Delogger.WorkingCopyCreated(ci) =>
        dos.write(JsonUtil.renderJson(ci).getBytes("UTF-8"))
      case Delogger.WorkingCopyPublished | Delogger.WorkingCopyDropped | Delogger.DataCopied | Delogger.Truncated =>
        /* pass */
      case Delogger.ColumnCreated(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.RowIdentifierSet(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.RowIdentifierCleared(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.ColumnRemoved(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.SystemRowIdentifierChanged(col) =>
        dos.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
      case Delogger.EndTransaction =>
        sys.error("Shouldn't have seen EndTransaction")
    }
  }

  def decode(stream: DataInputStream): Delogger.LogEvent[CV] = {
    val ev = eventType(stream)
    ev match {
      case "RowDataUpdated" =>
        val count = stream.readInt()
        val bytes = new Array[Byte](count)
        stream.readFully(bytes)
        Delogger.RowDataUpdated(bytes)(rowLogCodecFactory())
      case "RowIdCounterUpdated" =>
        val rid = new RowId(stream.readLong())
          Delogger.RowIdCounterUpdated(rid)
      case "WorkingCopyCreated" =>
        val ci = JsonUtil.readJson[CopyInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a copyinfo")
        }
        Delogger.WorkingCopyCreated(ci)
      case "WorkingCopyPublished" =>
        Delogger.WorkingCopyPublished
      case "WorkingCopyDropped" =>
        Delogger.WorkingCopyDropped
      case "DataCopied" =>
        Delogger.DataCopied
      case "Truncated" =>
        Delogger.Truncated
      case "ColumnCreated" =>
        val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.ColumnCreated(ci)
      case "RowIdentifierSet" =>
        val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.RowIdentifierSet(ci)
      case "RowIdentifierCleared" =>
        val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.RowIdentifierCleared(ci)
      case "ColumnRemoved" =>
        val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.ColumnRemoved(ci)
      case "SystemRowIdentifierChanged" =>
        val ci = JsonUtil.readJson[ColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
          throw new PacketDecodeException("Unable to decode a columnInfo")
        }
        Delogger.SystemRowIdentifierChanged(ci)
    }
  }

  def eventType(in: InputStream) = {
    val sb = new java.lang.StringBuilder
    def loop() {
      in.read() match {
        case -1 => throw new PacketDecodeException("LogData packet truncated before the event type")
        case 0 => // done
        case c => sb.append(c.toChar); loop()
      }
    }
    loop()
    sb.toString
  }
}
