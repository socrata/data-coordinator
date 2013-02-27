package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class PrimaryKeySetter(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  import mutator._
  def makePrimaryKey(dataset: String, column: String, username: String) {
    finish(dataset) {
      for {
        ctxOpt <- openDataset(as = username)(dataset)
        ctx <- ctxOpt
      } yield {
        import ctx._
        schema.values.find(_.logicalName == column) match {
          case Some(c) =>
            makeUserPrimaryKey(c)
          case None => sys.error("No such column") // TODO: better error
        }
      }
    }
  }
}
