package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.JValue
import com.rojoma.json.matcher.{PObject, Variable}
import com.socrata.datacoordinator.id.ColumnId

trait ColumnInfo {
  def versionInfo: VersionInfo
  def systemId: ColumnId
  def logicalName: String
  def typeName: String
  def isPrimaryKey: Boolean

  override final def hashCode = ScalaRunTime._hashCode((versionInfo, systemId, logicalName, typeName, isPrimaryKey))
  override final def equals(o: Any) = o match {
    case that: ColumnInfo =>
      (this eq that) ||
        (this.versionInfo == that.versionInfo && this.systemId == that.systemId && this.logicalName == that.logicalName && this.typeName == that.typeName && this.isPrimaryKey == that.isPrimaryKey)
    case _ =>
      false
  }
}

object ColumnInfo {
  implicit val jCodec = new JsonCodec[ColumnInfo] {
    val versionInfoV = Variable[VersionInfo]
    val systemIdV = Variable[ColumnId]
    val logicalNameV = Variable[String]
    val typeNameV = Variable[String]
    val isPrimaryKeyV = Variable[Boolean]

    val Pattern = new PObject(
      "versionInfo" -> versionInfoV,
      "systemId" -> systemIdV,
      "logicalName" -> logicalNameV,
      "typeName" -> typeNameV,
      "isPrimaryKey" -> isPrimaryKeyV
    )

    def encode(ci: ColumnInfo): JValue =
      Pattern.generate(
        versionInfoV := ci.versionInfo,
        systemIdV := ci.systemId,
        logicalNameV := ci.logicalName,
        typeNameV := ci.typeName,
        isPrimaryKeyV := ci.isPrimaryKey
      )

    def decode(x: JValue) = Pattern.matches(x) map { res =>
      new ColumnInfo {
        val versionInfo = versionInfoV(res)
        val systemId = systemIdV(res)
        val logicalName = logicalNameV(res)
        val typeName = typeNameV(res)
        val isPrimaryKey = isPrimaryKeyV(res)
      }
    }
  }
}
