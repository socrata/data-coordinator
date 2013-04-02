package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.id.{RowId, RowIdProcessor}
import com.rojoma.json.codec.JsonCodec

class SoQLRep(rowIdProcessor: RowIdProcessor) {
  val sqlRepFactories = Map[SoQLType, String => SqlColumnRep[SoQLType, Any]](
    SoQLID -> (base => new sqlreps.IDRep(base, rowIdProcessor)),
    SoQLText -> (base => new sqlreps.TextRep(base)),
    SoQLBoolean -> (base => new sqlreps.BooleanRep(base)),
    SoQLNumber -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLMoney -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLFixedTimestamp -> (base => new sqlreps.FixedTimestampRep(base)),
    SoQLLocation -> (base => new sqlreps.LocationRep(base)) /*,
    SoQLDouble -> doubleRepFactory,
    SoQLFloatingTimestamp -> floatingTimestampRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory */
  )

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))

  val csvRepFactories = Map[SoQLType, CsvColumnRep[SoQLType, Any]](
    SoQLID -> new csvreps.IDRep(rowIdProcessor),
    SoQLText -> csvreps.TextRep,
    SoQLBoolean -> csvreps.BooleanRep,
    SoQLNumber -> new csvreps.NumberLikeRep(SoQLNumber),
    SoQLMoney -> new csvreps.NumberLikeRep(SoQLMoney),
    SoQLFixedTimestamp -> csvreps.FixedTimestampRep,
    SoQLLocation -> csvreps.LocationRep
  )

  private def jsonRepFactories(codec: JsonCodec[RowId]) =
    Map[SoQLType, ColumnName => JsonColumnRep[SoQLType, Any]](
      SoQLID -> (name => new jsonreps.IDRep(name)(codec)),
      SoQLText -> (name => new jsonreps.TextRep(name)),
      SoQLBoolean -> (name => new jsonreps.BooleanRep(name)),
      SoQLNumber -> (name => new jsonreps.NumberLikeRep(name, SoQLNumber)),
      SoQLMoney -> (name => new jsonreps.NumberLikeRep(name, SoQLMoney)),
      SoQLFixedTimestamp -> (name => new jsonreps.FixedTimestampRep(name)),
      SoQLLocation -> (name => new jsonreps.LocationRep(name))
    )

  val obfuscatedJsonRepFactories = jsonRepFactories(rowIdProcessor.obfusactedCodec)
  val nonObfuscatedJsonRepFactories = jsonRepFactories(rowIdProcessor.nonObfusactedCodec)
}
