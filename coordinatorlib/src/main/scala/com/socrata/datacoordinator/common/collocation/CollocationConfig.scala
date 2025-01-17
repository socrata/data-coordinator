package com.socrata.datacoordinator.common.collocation

import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class CollocationConfig(config: Config, root: String) extends ConfigClass(config, root) {
  private def k(field: String) = root + "." + field
  val cost = new CollocationCostConfig(config, k("cost"))
  val group = optionally(getStringList("group")).fold(Set.empty[String])(_.toSet)
  val lockPath = getString("lock-path")
  val lockTimeout = getDuration("lock-timeout")
}

class CollocationCostConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val movesWeight = getString("moves-weight").toDouble
  val totalSizeBytesWeight = getString("total-size-bytes-weight").toDouble
  val moveSizeMaxBytesWeight = getString("move-size-max-bytes-weight").toDouble
}
