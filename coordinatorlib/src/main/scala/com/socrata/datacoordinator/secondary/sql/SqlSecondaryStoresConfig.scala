package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Timestamp, Connection}

import org.joda.time.DateTime
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.util.TimingReport

class SqlSecondaryStoresConfig(conn: Connection, timingReport: TimingReport) extends SecondaryStoresConfig {
  private def t = timingReport

  def lookup(storeId: String): Option[SecondaryConfigInfo] = {
    val sql = """
      SELECT store_id, next_run_time, interval_in_seconds, group_name, is_feedback_secondary
        FROM secondary_stores_config
       WHERE store_id = ?""".stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      _ <- unmanaged(stmt.setString(1, storeId))
      rs <- managed(stmt.executeQuery())
    } {
      if(rs.next()) {
        Some(SecondaryConfigInfo(
          rs.getString("store_id"),
          new DateTime(rs.getTimestamp("next_run_time").getTime),
          rs.getInt("interval_in_seconds"),
          Option(rs.getString("group_name")).getOrElse(""),
          rs.getBoolean("is_feedback_secondary")))
      } else {
        None
      }
    }
  }

  def create(secondaryInfo: SecondaryConfigInfo): SecondaryConfigInfo =
    using(conn.prepareStatement(
      """INSERT INTO secondary_stores_config (store_id
        |  ,next_run_time
        |  ,interval_in_seconds
        |) VALUES (?, ?, ?)""".stripMargin)) { stmt =>
      stmt.setString(1, secondaryInfo.storeId)
      stmt.setTimestamp(2, new Timestamp(secondaryInfo.nextRunTime.getMillis))
      stmt.setInt(3, secondaryInfo.runIntervalSeconds)
      stmt.execute()
      secondaryInfo
    }

  def updateNextRunTime(storeId: String, newNextRunTime: DateTime) {
    using(conn.prepareStatement(
      """UPDATE secondary_stores_config
        |SET next_run_time = ?
        |WHERE store_id = ?""".stripMargin)) { stmt =>
      stmt.setTimestamp(1, new Timestamp(newNextRunTime.getMillis))
      stmt.setString(2, storeId)
      t("update-next-runtime", "store-id" -> storeId)(stmt.execute())
    }
  }

  def group(storeId: String): Option[String] = {
    val sql = """
      SELECT group_name
        FROM secondary_stores_config
       WHERE store_id = ?""".stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      _ <- unmanaged(stmt.setString(1, storeId))
      rs <- managed(stmt.executeQuery())
    } {
      if(rs.next()) {
        Option(rs.getString("group_name"))
      } else {
        None
      }
    }
  }
}
