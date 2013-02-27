package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.MonadicDatasetMutator
import com.socrata.datacoordinator.truth.metadata.UnanchoredCopyInfo

class Publisher(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  def publish(dataset: String, username: String): UnanchoredCopyInfo = {
    finish(dataset) {
      for {
        ctxOpt <- mutator.openDataset(as = username)(dataset)
        ctx <- ctxOpt
      } yield {
        ctx.publish().unanchored
      }
    }
  }
}
