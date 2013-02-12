package com.socrata.datacoordinator.packets

import java.io.{InputStream, OutputStream}
import scala.concurrent.duration.Duration
import java.nio.ByteBuffer

object PacketsStream {
  val defaultDataLabel = "stream-data"
  val defaultEndLabel = "end-stream"

  class DataPacket(label: String = defaultDataLabel) extends Packet.SimpleLabelledPacket(label)
  class EndPacket(label: String = defaultEndLabel) extends Packet.SimplePacket(label)
}

class PacketsInputStream(packets: Packets,
                         dataLabel: String = PacketsStream.defaultDataLabel,
                         endLabel: String = PacketsStream.defaultEndLabel,
                         readTimeout: Duration = Duration.Inf)
  extends InputStream
{
  val Data = new PacketsStream.DataPacket(dataLabel)
  val End = new PacketsStream.EndPacket(endLabel)

  private var currentBuffer = receive()

  def read(): Int =
    if(currentBuffer == null) -1
    else {
      val result = currentBuffer.get() & 0xff
      maybeReceive()
      result
    }

  override def read(bs: Array[Byte], offset: Int, length: Int): Int =
    if(currentBuffer == null) -1
    else {
      val result = length.min(currentBuffer.remaining())
      currentBuffer.get(bs, offset, result)
      maybeReceive()
      result
    }

  private def maybeReceive() {
    if(!currentBuffer.hasRemaining) currentBuffer = receive()
  }

  private def receive(): ByteBuffer = {
    packets.receive(readTimeout) match {
      case Some(Data(data)) =>
        data
      case Some(End()) =>
        null
      case None =>
        sys.error("End of input received?") // TODO: Better error
    }
  }
}

object PacketsInputStream {
  def isStreamPacket(p: Packet) = p.dataSize > 0 && {
    val b = p.data
    b.get(0) == 0 || b.get(0) == 1
  }
}

class PacketsOutputStream(packets: Packets,
                          dataLabel: String = PacketsStream.defaultDataLabel,
                          endLabel: String = PacketsStream.defaultEndLabel,
                          writeTimeout: Duration = Duration.Inf,
                          postWrite: () => Unit = PacketsOutputStream.Noop)
  extends OutputStream
{
  require(packets.maxPacketSize >= PacketsOutputStream.minimumSizeFor(dataLabel, endLabel),
    "Max packet size not large enough for the data stream packets")

  val Data = new PacketsStream.DataPacket(dataLabel)
  val End = new PacketsStream.EndPacket(endLabel)

  private var currentPacket: PacketOutputStream = null

  def freshPacket() = Data.packetOutputStream()

  def write(b: Int) {
    if(currentPacket == null) currentPacket = freshPacket()
    currentPacket.write(b)
    maybeFlush()
  }

  override def write(bs: Array[Byte], offset: Int, length: Int) {
    var hd = offset
    var rem = length
    while(rem > 0) {
      if(currentPacket == null) currentPacket = freshPacket()
      val toWrite = rem.min(packets.maxPacketSize - currentPacket.size)
      currentPacket.write(bs, hd, toWrite)
      maybeFlush()
      hd += toWrite
      rem -= toWrite
    }
  }

  private def maybeFlush() {
    if(currentPacket.size >= packets.maxPacketSize) flush()
  }

  override def flush() {
    if(currentPacket != null) {
      packets.send(currentPacket.packet(), writeTimeout)
      postWrite()
      currentPacket = null
    }
  }

  override def close() {
    flush()
    packets.send(End(), writeTimeout)
    postWrite()
  }
}

object PacketsOutputStream {
  private val Noop = () => ()
  def minimumSizeFor(dataLabel: String, endLabel: String): Int =
    math.max(Packet.SimpleLabelledPacket.minimumSizeFor(dataLabel) + 1,
      Packet.SimplePacket.minimumSizeFor(endLabel))
}
