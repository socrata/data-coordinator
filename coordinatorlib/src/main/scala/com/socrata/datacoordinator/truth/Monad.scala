package com.socrata.datacoordinator
package truth

import org.joda.time.DateTime
import com.rojoma.simplearm.{SimpleArm, Managed}

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.RowId

trait LowLevelMonadicDatabaseReader[CV] {
  trait ReadContext {
    def datasetMap: DatasetMapReader

    final def loadDataset(datasetName: String, latest: Boolean): Option[(CopyInfo, ColumnIdMap[ColumnInfo])] = {
      val map = datasetMap
      for {
        datasetInfo <- map.datasetInfo(datasetName)
        copyInfo <- if(latest) Some(map.latest(datasetInfo)) else map.published(datasetInfo)
      } yield (copyInfo, map.schema(copyInfo))
    }

    def withRows[A](ci: CopyInfo, schema: ColumnIdMap[ColumnInfo], f: Iterator[ColumnIdMap[CV]] => A): A
  }

  def openDatabase: Managed[ReadContext]
}

trait LowLevelMonadicDatabaseMutator[CV] {
  trait MutationContext {
    def now: DateTime
    def datasetMap: DatasetMapWriter
    def logger(info: DatasetInfo): Logger[CV]
    def schemaLoader(logger: Logger[CV]): SchemaLoader
    def datasetContentsCopier(logger: Logger[CV]): DatasetContentsCopier
    def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: Loader[CV] => A): (Report[CV], RowId, A)

    def globalLog: GlobalLog

    final def finishDatasetTransaction(username: String, copyInfo: CopyInfo, logger: Logger[CV]) {
      logger.endTransaction() foreach { ver =>
        datasetMap.updateDataVersion(copyInfo, ver)
        globalLog.log(copyInfo.datasetInfo, ver, now, username)
      }
    }

    final def loadLatestVersionOfDataset(datasetName: String): Option[(CopyInfo, ColumnIdMap[ColumnInfo])] = {
      val map = datasetMap
      map.datasetInfo(datasetName) map { datasetInfo =>
        val latest = map.latest(datasetInfo)
        val schema = map.schema(latest)
        (latest, schema)
      }
    }
  }

  def openDatabase: Managed[MutationContext]
}

trait MonadicDatasetReader[CV] {
  val databaseReader: LowLevelMonadicDatabaseReader[CV]

  trait ReadContext {
    val copyInfo: CopyInfo
    val schema: ColumnIdMap[ColumnInfo]
    def withRows[A](f: Iterator[ColumnIdMap[CV]] => A): A // TODO: I think this should return a Managed[Iterator...]
  }

  /**
   * @param latest If false, this action operates on the published version even if there
   *               is a newer working copy.
   */
  def openDataset(datasetName: String, latest: Boolean): Managed[Option[ReadContext]]
}

object MonadicDatasetReader {
  private class Impl[CV](val databaseReader: LowLevelMonadicDatabaseReader[CV]) extends MonadicDatasetReader[CV] {
    class S(val copyInfo: CopyInfo, val schema: ColumnIdMap[ColumnInfo], llCtx: databaseReader.ReadContext) extends ReadContext {
      def withRows[A](f: Iterator[ColumnIdMap[CV]] => A): A =
        llCtx.withRows(copyInfo, schema, f)
    }

    def openDataset(datasetName: String, latest: Boolean): Managed[Option[ReadContext]] =
      new SimpleArm[Option[ReadContext]] {
        def flatMap[A](f: Option[ReadContext] => A): A = for {
          llCtx <- databaseReader.openDatabase
        } yield {
          val ctx = llCtx.loadDataset(datasetName, latest) map { case (initialCopy, initialSchema) =>
            new S(initialCopy, initialSchema, llCtx)
          }
          f(ctx)
        }
      }
  }

  def apply[CV](lowLevelReader: LowLevelMonadicDatabaseReader[CV]): MonadicDatasetReader[CV] = new Impl(lowLevelReader)
}

trait MonadicDatasetMutator[CV] {
  val databaseMutator: LowLevelMonadicDatabaseMutator[CV]

  trait MutationContext {
    def copyInfo: CopyInfo
    def schema: ColumnIdMap[ColumnInfo]

    final def schemaByLogicalName: Map[String, ColumnInfo] =
      schema.values.foldLeft(Map.empty[String, ColumnInfo]) { (acc, ci) =>
        acc + (ci.logicalName -> ci)
      }

    def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): ColumnInfo
    def makeSystemPrimaryKey(ci: ColumnInfo): ColumnInfo
    def makeUserPrimaryKey(ci: ColumnInfo): ColumnInfo
    def dropColumn(ci: ColumnInfo): Unit
    def publish(): CopyInfo
    def upsert(inputGenerator: Iterator[Either[CV, Row[CV]]]): Report[CV]
    def drop() // TODO: this should be a top-level thing
  }

  def createDataset(as: String)(datasetName: String, tableBaseBase: String): Managed[MutationContext]
  def openDataset(as: String)(datasetName: String): Managed[Option[MutationContext]]
  def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[Option[MutationContext]]
}

object MonadicDatasetMutator {
  private class Impl[CV](val databaseMutator: LowLevelMonadicDatabaseMutator[CV]) extends MonadicDatasetMutator[CV] {
    class S(var copyInfo: CopyInfo, var schema: ColumnIdMap[ColumnInfo], val schemaLoader: SchemaLoader, val logger: Logger[CV], llCtx: databaseMutator.MutationContext) extends MutationContext {
      def now = llCtx.now
      def datasetMap = llCtx.datasetMap
      def datasetContetsCopier = llCtx.datasetContentsCopier(logger)

      def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): ColumnInfo = {
        val newColumn = datasetMap.addColumn(copyInfo, logicalName, typeName, physicalColumnBaseBase)
        schemaLoader.addColumn(newColumn)
        schema += newColumn.systemId -> newColumn
        newColumn
      }

      def dropColumn(ci: ColumnInfo) {
        datasetMap.dropColumn(ci)
        schemaLoader.dropColumn(ci)
        schema -= ci.systemId
      }

      def makeSystemPrimaryKey(ci: ColumnInfo): ColumnInfo = {
        val result = datasetMap.setSystemPrimaryKey(ci)
        val ok = schemaLoader.makeSystemPrimaryKey(result)
        require(ok, "Column cannot be made a system primary key")
        schema += result.systemId -> result
        result
      }

      def makeUserPrimaryKey(ci: ColumnInfo): ColumnInfo = {
        val result = datasetMap.setUserPrimaryKey(ci)
        val ok = schemaLoader.makePrimaryKey(result)
        require(ok, "Column cannot be made a primary key")
        schema += result.systemId -> result
        result
      }

      def datasetContentsCopier = llCtx.datasetContentsCopier(logger)

      def makeWorkingCopy(copyData: Boolean): CopyInfo = {
        val dataCopier = if(copyData) Some(datasetContentsCopier) else None
        datasetMap.ensureUnpublishedCopy(copyInfo.datasetInfo) match {
          case Left(_) =>
            sys.error("Already a working copy") // TODO: Better error
          case Right(CopyPair(oldCopy, newCopy)) =>
            assert(oldCopy == copyInfo)

            // Great.  Now we can actually do the data loading.
            schemaLoader.create(newCopy)
            val newSchema = datasetMap.schema(newCopy)
            for(ci <- newSchema.values) {
              schemaLoader.addColumn(ci)
            }

            dataCopier.foreach(_.copy(oldCopy, newCopy, schema))

            val finalSchema = new MutableColumnIdMap[ColumnInfo]
            for(ci <- newSchema.values) {
              val ci2 = if(schema(ci.systemId).isSystemPrimaryKey) {
                val pkified = datasetMap.setSystemPrimaryKey(ci)
                schemaLoader.makeSystemPrimaryKey(pkified)
                pkified
              } else {
                ci
              }

              val ci3 = if(schema(ci2.systemId).isUserPrimaryKey) {
                val pkified = datasetMap.setUserPrimaryKey(ci2)
                schemaLoader.makePrimaryKey(pkified)
                pkified
              } else {
                ci2
              }
              finalSchema(ci3.systemId) = ci3
            }

            copyInfo = newCopy
            schema = finalSchema.freeze()
            newCopy
        }
      }

      def publish(): CopyInfo = {
        val newCi = datasetMap.publish(copyInfo)
        logger.workingCopyPublished()
        copyInfo = newCi
        schema = datasetMap.schema(newCi)
        copyInfo
      }

      def upsert(inputGenerator: Iterator[Either[CV, Row[CV]]]): Report[CV] = {
        val (report, nextRowId, _) = llCtx.withDataLoader(copyInfo, schema, logger) { loader =>
          inputGenerator.foreach {
            case Right(row) => loader.upsert(row)
            case Left(id) => loader.delete(id)
          }
        }
        copyInfo = datasetMap.updateNextRowId(copyInfo, nextRowId)
        report
      }

      def drop() {
        datasetMap.dropCopy(copyInfo)
        schemaLoader.drop(copyInfo)
        copyInfo = datasetMap.latest(copyInfo.datasetInfo)
        schema = datasetMap.schema(copyInfo)
      }
    }

    private def go[A](username: String, datasetName: String, action: Option[S] => A): A =
      for {
        llCtx <- databaseMutator.openDatabase
      } yield {
        val ctx = llCtx.loadLatestVersionOfDataset(datasetName) map { case (initialCopy, initialSchema) =>
          val logger = llCtx.logger(initialCopy.datasetInfo)
          val schemaLoader = llCtx.schemaLoader(logger)
          new S(initialCopy, initialSchema, schemaLoader, logger, llCtx)
        }
        val result = action(ctx)
        ctx.foreach { state =>
          llCtx.finishDatasetTransaction(username, state.copyInfo, state.logger)
        }
        result
      }

    def openDataset(as: String)(datasetName: String): Managed[Option[MutationContext]] = new SimpleArm[Option[MutationContext]] {
      def flatMap[A](f: Option[MutationContext] => A): A =
        go(as, datasetName, f)
    }

    def createDataset(as: String)(datasetName: String, tableBaseBase: String): Managed[MutationContext] = new SimpleArm[MutationContext] {
      def flatMap[A](f: MutationContext => A): A =
        for { llCtx <- databaseMutator.openDatabase } yield {
          val m = llCtx.datasetMap
          val firstVersion = m.create(datasetName, tableBaseBase)
          val logger = llCtx.logger(firstVersion.datasetInfo)
          val schemaLoader = llCtx.schemaLoader(logger)
          schemaLoader.create(firstVersion)
          val state = new S(firstVersion, ColumnIdMap.empty, schemaLoader, logger, llCtx)
          val result = f(state)
          llCtx.finishDatasetTransaction(as, state.copyInfo, logger)
          result
        }
    }

    def createCopy(as: String)(datasetName: String, copyData: Boolean): Managed[Option[MutationContext]] = new SimpleArm[Option[MutationContext]] {
      def flatMap[A](f: Option[MutationContext] => A): A =
        go(as,datasetName, { ctxOpt =>
          ctxOpt.foreach(_.makeWorkingCopy(copyData))
          f(ctxOpt)
        })
    }
  }

  def apply[CV](lowLevelMutator: LowLevelMonadicDatabaseMutator[CV]): MonadicDatasetMutator[CV] = new Impl(lowLevelMutator)
}
