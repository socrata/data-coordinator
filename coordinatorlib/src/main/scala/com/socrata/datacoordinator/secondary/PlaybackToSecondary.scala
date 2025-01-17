package com.socrata.datacoordinator
package secondary

import java.sql.SQLException

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.loader.{Delogger, MissingVersion}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.soql.environment.TypeName
import com.socrata.thirdparty.metrics.Metrics
import org.postgresql.util.PSQLException
import scala.concurrent.duration.Duration
import scala.util.control.ControlThrowable
import org.slf4j.LoggerFactory

object PlaybackToSecondary {
  type SuperUniverse[CT, CV] = Universe[CT, CV] with Commitable with
                                                     SecondaryStoresConfigProvider with
                                                     SecondaryManifestProvider with
                                                     SecondaryMetricsProvider with
                                                     DatasetMapReaderProvider with
                                                     DatasetMapWriterProvider with
                                                     DatasetReaderProvider with
                                                     DeloggerProvider
  private val logger = LoggerFactory.getLogger(classOf[PlaybackToSecondary[_, _]])
}

class PlaybackToSecondary[CT, CV](u: PlaybackToSecondary.SuperUniverse[CT, CV],
                                  repFor: metadata.ColumnInfo[CT] => SqlColumnReadRep[CT, CV],
                                  typeForName: TypeName => Option[CT],
                                  datasetIdFormatter: DatasetId => String,
                                  timingReport: TimingReport) {
  import PlaybackToSecondary.logger

  val datasetLockTimeout = Duration.Inf

  class LifecycleStageTrackingIterator(underlying: Iterator[Delogger.LogEvent[CV]],
                                       initialStage: metadata.LifecycleStage)
      extends BufferedIterator[Delogger.LogEvent[CV]] {
    private var currentStage = initialStage
    private var lookahead: Delogger.LogEvent[CV] = null

    def stageBeforeNextEvent: metadata.LifecycleStage = currentStage

    def stageAfterNextEvent: metadata.LifecycleStage = computeNextStage(head)

    def hasNext: Boolean = lookahead != null || underlying.hasNext

    def head: Delogger.LogEvent[CV] = {
      if(lookahead == null) lookahead = advance()
      lookahead
    }

    private def advance() = underlying.next()

    def next(): Delogger.LogEvent[CV] = {
      val ev =
        if(lookahead == null) {
          advance()
        } else {
          val peeked = lookahead
          lookahead = null
          peeked
        }
      currentStage = computeNextStage(ev)
      ev
    }

    private def computeNextStage(ev: Delogger.LogEvent[CV]) =
      ev match {
        case Delogger.WorkingCopyCreated(_, _) =>
          metadata.LifecycleStage.Unpublished
        case Delogger.WorkingCopyDropped | Delogger.WorkingCopyPublished =>
          metadata.LifecycleStage.Published
        case _ =>
          currentStage
      }
  }

  // This is guaranteed to consume no more than necessary out of the iterator.
  // In particular, when it is done, the underlying iterator will either be empty
  // or positioned so that next() is a stage-changing event.
  class StageLimitedIterator(underlying: LifecycleStageTrackingIterator) extends Iterator[Delogger.LogEvent[CV]] {
    private val wantedStage = underlying.stageBeforeNextEvent

    def hasNext: Boolean = underlying.hasNext && underlying.stageAfterNextEvent == wantedStage
    def next(): Delogger.LogEvent[CV] =
      if(hasNext) underlying.next()
      else Iterator.empty.next()

    def finish(): Unit = while(hasNext) next()
  }

  // Instruments via metrics and logging the iteration of rows/events
  class InstrumentedIterator[T](name: String,
                                datasetName: String,
                                underlying: Iterator[T],
                                loggingRate: Int = 10000) extends Iterator[T] with Metrics {
    var itemNum = 0
    val meter = metrics.meter(name + ".rows")

    def hasNext: Boolean = underlying.hasNext
    def next(): T = {
      meter.mark()
      itemNum += 1
      if(itemNum % loggingRate == 0)
        logger.info("[{}] {}: {} rows/events processed", name, datasetName, itemNum.toString)
      underlying.next()
    }
  }

  class DeactivateAutoCommit extends AutoCloseable {
    private var done = false
    override def close() {
      if(!done) {
        u.autoCommit(false)
        done = true
      }
    }
  }

  def apply(secondary: NamedSecondary[CT, CV], job: SecondaryRecord): Unit = {
    logger.info("Ending current transaction and switching to auto-commit mode")
    u.commit()

    using(new DeactivateAutoCommit) { dac =>
      u.autoCommit(true)
      if (job.pendingDrop) {
        logger.info("Executing pending drop of dataset {} from store {}.", job.datasetId : Any, job.storeId)
        new UpdateOp(secondary, job, dac).drop()
      } else {
        new UpdateOp(secondary, job, dac).go()
      }
    }
  }

  def makeSecondaryDatasetInfo(dsInfo: metadata.DatasetInfoLike) =
    DatasetInfo(datasetIdFormatter(dsInfo.systemId), dsInfo.localeName, dsInfo.obfuscationKey.clone(), dsInfo.resourceName.underlying)

  def makeSecondaryCopyInfo(copyInfo: metadata.CopyInfoLike) =
    CopyInfo(copyInfo.systemId,
             copyInfo.copyNumber,
             copyInfo.lifecycleStage.correspondingSecondaryStage,
             copyInfo.dataVersion,
             copyInfo.dataShapeVersion,
             copyInfo.lastModified)

  def makeSecondaryRollupInfo(rollupInfo: metadata.RollupInfoLike) =
    RollupInfo(rollupInfo.name.underlying, rollupInfo.soql)

  def makeSecondaryColumnInfo(colInfo: metadata.ColumnInfoLike) = {
    typeForName(TypeName(colInfo.typeName)) match {
      case Some(typ) =>
        ColumnInfo(colInfo.systemId,
                   colInfo.userColumnId,
                   colInfo.fieldName,
                   typ,
                   isSystemPrimaryKey = colInfo.isSystemPrimaryKey,
                   isUserPrimaryKey = colInfo.isUserPrimaryKey,
                   isVersion = colInfo.isVersion,
                   colInfo.computationStrategyInfo.map { strategy =>
                     ComputationStrategyInfo(strategy.strategyType, strategy.sourceColumnIds, strategy.parameters)
                   })
      case None =>
        sys.error("Typename " + colInfo.typeName + " got into the logs somehow!")
    }
  }

  def makeSecondaryIndexInfo(indexInfo: metadata.IndexInfoLike) =
    IndexInfo(indexInfo.systemId, indexInfo.name.underlying, indexInfo.expressions, indexInfo.filter)

  private class UpdateOp(secondary: NamedSecondary[CT, CV],
                         job: SecondaryRecord,
                         deactivateAutoCommit: DeactivateAutoCommit)
  {
    private val datasetId = job.datasetId
    private val claimantId = job.claimantId
    private var currentCookie = job.initialCookie
    private val datasetMapReader = u.datasetMapReader

    def go(): Unit = {
      datasetMapReader.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          logger.info("Found dataset " + datasetInfo.systemId + " in truth")
          try {
            reconsolidateAndPlayback(datasetInfo)
          } catch {
            case e: MissingVersion =>
              logger.info("Couldn't find version {} in log; resyncing", e.version.toString)
              deactivateAutoCommit.close()
              resyncSerially()
            case ResyncSecondaryException(reason) =>
              logger.info("Incremental update requested full resync: {}", reason)
              deactivateAutoCommit.close()
              resyncSerially()
          }

          saveMetrics(datasetInfo)
        case None =>
          drop()
      }
    }


    def reconsolidateAndPlayback(datasetInfo: metadata.DatasetInfo): Unit = {
      var dataVersion = job.startingDataVersion
      while(dataVersion <= job.endingDataVersion) {
        logger.trace("Playing back version {}", dataVersion)
        using(u.delogger(datasetInfo)) { delogger =>
          val lastConsolidatableVersion: Option[Long] =
            // We'll scan over the range of versions, eliminating
            // possibilities for consolidation until there's nothing
            // left, and then play them all back together.
            (dataVersion to job.endingDataVersion).iterator.scanLeft((-1L, Consolidatable.all)) { (dvAcc, dv) =>
              val (_, acc) = dvAcc
              using(delogger.delogOnlyTypes(dv)) { it =>
                (dv, acc.intersect(consolidatable(it.buffered)))
              }
            }.drop(1).takeWhile(_._2.nonEmpty).map(_._1).toStream.lastOption

          lastConsolidatableVersion match {
            case Some(n) if n > dataVersion =>
              // at least two versions can be consolidated into one
              logger.info("Consolidating data versions {} through {} into a single thing", dataVersion, n)
              val rollups = extractRollups(delogger, dataVersion, n)
              using(consolidate(delogger, dataVersion, n)) { it =>
                playbackLog(datasetInfo, it, dataVersion, n, rollups)
              }
              dataVersion = n
            case _ =>
              // Zero or one version can be consolidated into one
              val rollups = extractRollups(delogger, dataVersion, dataVersion)
              using(delogger.delog(dataVersion)) { it =>
                playbackLog(datasetInfo, it, dataVersion, dataVersion, rollups)
              }
          }
        }
        updateSecondaryMap(dataVersion)
        dataVersion += 1
      }
    }

    sealed abstract class Consolidatable
    object Consolidatable {
      def all = Set[Consolidatable](Rows, Rollups)
    }
    case object Rows extends Consolidatable
    case object Rollups extends Consolidatable

    // A series of events is consolidatable if it is an update which only
    // changes row data or rollups, which is to say if it has the form:
    //    RowsChangedPreview
    //    zero or one Truncated
    //    zero or more RowDataUpdated
    //    LastModifiedChanged
    // or
    //    zero or more intermingled RollupCreatedOrUpdated and RollupDropped
    //    LastModifiedChanged
    // or
    //    LastModifiedChanged
    def consolidatable(it: BufferedIterator[Delogger.LogEventCompanion]): Set[Consolidatable] = {
      if(it.hasNext && it.head == Delogger.LastModifiedChanged) {
        it.next()
        if(it.hasNext) {
          return Set.empty
        } else {
          // Empty changeset, can be consolidated with anything
          return Consolidatable.all
        }
      }

      if(!it.hasNext) {
        return Set.empty
      }

      it.next() match {
        case Delogger.RollupCreatedOrUpdated | Delogger.RollupDropped =>
          while(it.hasNext && (it.head == Delogger.RollupCreatedOrUpdated || it.head == Delogger.RollupDropped)) {
            it.next()
          }
          if(!it.hasNext || it.next() != Delogger.LastModifiedChanged) {
            return Set.empty
          }
          if(it.hasNext) Set.empty // didn't end with LastModifiedChanged?
          else Set(Rollups)

        case Delogger.RowsChangedPreview =>
          if(it.hasNext && it.head == Delogger.Truncated) {
            it.next()
          }
          while(it.hasNext && it.head == Delogger.RowDataUpdated) {
            it.next()
          }
          if(!it.hasNext || it.next() != Delogger.LastModifiedChanged) {
            return Set.empty
          }

          if(it.hasNext) Set.empty // didn't end with LastModifiedChanged?
          else Set(Rows)

        case _ =>
          Set.empty
      }
    }

    def consolidate(delogger: Delogger[CV], startingDataVersion: Long, endingDataVersion: Long) =
      new Iterator[Delogger.LogEvent[CV]] with AutoCloseable {
        // ok, so we're going to want to produce a fake event stream
        // shaped like a consolidatable (see above) version.  To do
        // this, we'll combine all the versions' RowsChangedPreview
        // events then the concatenation of their RowDataUpdateds,
        // then the final job's LastModifiedChanged.

        val rs = new ResourceScope

        sealed abstract class State
        case object Unknown extends State
        case class ConsolidatingRows(preview: Delogger.RowsChangedPreview) extends State
        case object ConsolidatingRollups extends State

        val (effectiveStartingDataVersion, state) =
          (startingDataVersion to endingDataVersion).foldLeft((startingDataVersion, Unknown:State)) { (acc, i) =>
            val (currentStartingDataVersion, state) = acc

            def rcpOfState() =
              state match {
                case Unknown => Delogger.RowsChangedPreview(0, 0, 0, false)
                case ConsolidatingRows(rcp) => rcp
                case ConsolidatingRollups => throw new ResyncSecondaryException("Trying to consolidate rows, but current state is for rollups?")
              }
            def ensureRollups() =
              state match {
                case Unknown | ConsolidatingRollups => ConsolidatingRollups
                case ConsolidatingRows(_) => throw new ResyncSecondaryException("Trying to consoldate rollups, but current state is for rows?")
              }

            managed(delogger.delog(i)).run(_.buffered.headOption) match {
              case Some(Delogger.RowsChangedPreview(rowsInserted, rowsUpdated, rowsDeleted, false)) =>
                val rcp = rcpOfState()
                (currentStartingDataVersion, ConsolidatingRows(Delogger.RowsChangedPreview(rcp.rowsInserted + rowsInserted,
                                                                                           rcp.rowsUpdated + rowsUpdated,
                                                                                           rcp.rowsDeleted + rowsDeleted,
                                                                                           rcp.truncated)))
              case Some(rcp : Delogger.RowsChangedPreview) =>
                rcpOfState() // ensure we don't think we're looking at rollups
                // it was truncated; just start from here
                (i, ConsolidatingRows(rcp))
              case Some(Delogger.LastModifiedChanged(_)) =>
                // no change -- just keep acc
                (currentStartingDataVersion, state)
              case Some(Delogger.RollupCreatedOrUpdated(_) | Delogger.RollupDropped(_)) =>
                (currentStartingDataVersion, ensureRollups())
              case _ =>
                throw ResyncSecondaryException("Consolidation saw the log change?!  No RowsChangedPreview!")
            }
          }

        val finalIterator =
          state match {
            case Unknown =>
              // bunch of empty updates...?  Ok.
              consolidateNothing(delogger, effectiveStartingDataVersion, endingDataVersion, rs)
            case ConsolidatingRows(fakeRCP) =>
              consolidateRows(fakeRCP, delogger, effectiveStartingDataVersion, endingDataVersion, rs)
            case ConsolidatingRollups =>
              consolidateRollups(delogger, effectiveStartingDataVersion, endingDataVersion, rs)
          }

        def hasNext = finalIterator.hasNext
        def next() = finalIterator.next()

        def close() {
          rs.close()
        }
      }

    private def consolidateNothing(delogger: Delogger[CV], startingDataVersion: Long, endingDataVersion: Long, rs: ResourceScope): Iterator[Delogger.LogEvent[CV]] = {
      val data = (startingDataVersion to endingDataVersion).iterator.flatMap { i =>
        new Iterator[Delogger.LogEvent[CV]] {
          var done = false
          val eventsRaw = rs.open(delogger.delog(i))
          val events = eventsRaw.buffered

          def hasNext: Boolean = {
            if(done) return false
            events.headOption match {
              case Some(Delogger.LastModifiedChanged(_)) =>
                true
              case Some(other) =>
                throw ResyncSecondaryException(s"Consolidation saw the log change?!  Saw s{other.companion.productPrefix} while expecting LastModifiedChanged!")
              case None =>
                done = true
                rs.close(eventsRaw)
                false
            }
          }

          def next() = {
            if(hasNext) events.next()
            else Iterator.empty.next()
          }
        }
      }
      if(data.hasNext) Iterator.single(data.toStream.last)
      else throw ResyncSecondaryException(s"Consolidation saw the log change?!  No LastModifiedChanged!")
    }

    private def consolidateRows(fakeRCP: Delogger.RowsChangedPreview, delogger: Delogger[CV], startingDataVersion: Long, endingDataVersion: Long, rs: ResourceScope): Iterator[Delogger.LogEvent[CV]] = {
      // bleargghhgh -- annoying that this super lazy flatmap over
      // the data versions has a side effect, but I can't think of a
      // clearer way to do this.
      var mostRecentLastModified: Option[Delogger.LastModifiedChanged] = None
      val data = (startingDataVersion to endingDataVersion).iterator.flatMap { i =>
        new Iterator[Delogger.LogEvent[CV]] {
          var done: Boolean = false

          val eventsRaw = rs.open(delogger.delog(i))
          val events = eventsRaw.buffered

          if(events.hasNext && events.head.companion == Delogger.LastModifiedChanged) {
            // empty changeset.
          } else {
            if(!events.hasNext || events.head.companion != Delogger.RowsChangedPreview) {
              throw ResyncSecondaryException("Consolidation saw the log change?!  First item was gone or not RowsChangedPreview!")
            }
            if(events.next().asInstanceOf[Delogger.RowsChangedPreview].truncated) {
              if(!events.hasNext || events.next().companion != Delogger.Truncated) {
                throw ResyncSecondaryException("RowsChangedPreview said the dataset was to be truncated, but there was no Truncated event!")
              }
              if(mostRecentLastModified.isDefined) {
                throw ResyncSecondaryException("Dataset was truncated but it wasn't the first batch we were looking at!")
              }
            }
          }

          def hasNext: Boolean = {
            if(done) return false

            events.headOption match {
              case Some(v@Delogger.RowDataUpdated(_)) =>
                true

              case Some(lmc@Delogger.LastModifiedChanged(_)) =>
                mostRecentLastModified = Some(lmc) // this is the reason for the bleargghhgh before
                done = true
                ensureEnd(events)
                rs.close(eventsRaw)
                false

              case Some(other) =>
                throw ResyncSecondaryException(s"Consolidation saw the log change?!  Saw s{other.companion.productPrefix} while expecting RowsChangedPreview or LastModifiedChanged!")

              case None =>
                throw ResyncSecondaryException("Consolidation saw the log change?!  Reached EOF while expecting RowsChangedPreview or LastModifiedChanged!")
            }
          }

          def next() = {
            if(hasNext) events.next()
            else Iterator.empty.next()
          }
        }
      }

      Iterator(fakeRCP) ++
        (if(fakeRCP.truncated) Iterator(Delogger.Truncated) else Iterator.empty) ++
        data ++
        // bleargghhgh part 3: Iterator#++ is sufficiently lazy
        // for this lookup of the var that's modified by iterating
        // through `data` to work.
        Iterator(mostRecentLastModified.getOrElse {
                   throw ResyncSecondaryException("Consolidation saw the log change - no LastModifiedChanged?!")
                 })
    }

    private def consolidateRollups(delogger: Delogger[CV], startingDataVersion: Long, endingDataVersion: Long, rs: ResourceScope): Iterator[Delogger.LogEvent[CV]] = {
      // Same "bleargh" as in consolidateRows re: iterator operation
      // with this mutable thing.  At least consolidating rollups is
      // way simpler...
      var mostRecentLastModified: Option[Delogger.LastModifiedChanged] = None
      val data = (startingDataVersion to endingDataVersion).iterator.flatMap { i =>
        new Iterator[Delogger.LogEvent[CV]] {
          var done: Boolean = false

          val eventsRaw = rs.open(delogger.delog(i))
          val events = eventsRaw.buffered

          if(events.hasNext && events.head.companion == Delogger.LastModifiedChanged) {
            // empty changeset.
          } else if(!events.hasNext || (events.head.companion != Delogger.RollupCreatedOrUpdated && events.head.companion != Delogger.RollupDropped)) {
            throw ResyncSecondaryException("Consolidation saw the log change?!  First item was gone or not a rollup event!")
          }

          def hasNext: Boolean = {
            if(done) return false
            events.headOption match {
              case Some(Delogger.RollupCreatedOrUpdated(_) | Delogger.RollupDropped(_)) =>
                true
              case Some(lmc@Delogger.LastModifiedChanged(_)) =>
                mostRecentLastModified = Some(lmc)
                done = true
                ensureEnd(events)
                rs.close(eventsRaw)
                false

              case Some(other) =>
                throw ResyncSecondaryException(s"Consolidation saw the log change?!  Saw s{other.companion.productPrefix} while expecting RollupCreatedOrUpdated, RollupDropped, or LastModifiedChanged!")

              case None =>
                throw ResyncSecondaryException("Consolidation saw the log change?!  Reached EOF while expecting RollupCreatedOrUpdated, RollupDropped, or LastModifiedChanged!")
            }
          }

          def next() = {
            if(hasNext) events.next()
            else Iterator.empty.next()
          }
        }
      }

      data ++ Iterator(mostRecentLastModified.getOrElse {
                         throw new ResyncSecondaryException("Consolidation saw the log change - no LastModifiedChanged?!")
                       })
    }

    // This is called when the iterator is focused on a
    // LastModifiedChanged to ensure it's the final event.
    private def ensureEnd(events: BufferedIterator[Delogger.LogEvent[CV]]): Unit = {
      events.next() // skip LastModifiedChanged
      events.headOption match {
        case None =>
        // ok good
        case Some(other) =>
          throw ResyncSecondaryException(s"Consolidation saw the log change?!  Saw s{other.companion.productPrefix} while expecting EndTransaction!")
      }
    }

    def extractRollups(
      delogger: Delogger[CV],
      startDataVersion: Long,
      endDataVersion: Long
    ): Seq[RollupInfo] = {
      delogger.findCreateRollupEvents(startDataVersion, endDataVersion).map { ev =>
        makeSecondaryRollupInfo(ev.info)
      }
    }

    def playbackLog(
      datasetInfo: metadata.DatasetInfo,
      it: Iterator[Delogger.LogEvent[CV]],
      startDataVersion: Long,
      endDataVersion: Long,
      newRollups: Seq[RollupInfo]
    ) {
      val secondaryDatasetInfo = makeSecondaryDatasetInfo(datasetInfo)
      val instrumentedIt = new InstrumentedIterator("playback-log-throughput",
                                                    datasetInfo.systemId.toString,
                                                    it)
      currentCookie = secondary.store.version(
        new VersionInfo[CT, CV] {
          override val datasetInfo = secondaryDatasetInfo
          override val initialDataVersion = startDataVersion
          override val finalDataVersion = endDataVersion
          override val cookie = currentCookie
          override val createdOrUpdatedRollups = newRollups
          override val events = instrumentedIt.flatMap(convertEvent)
        }
      )
    }

    def convertOp(op: truth.loader.Operation[CV]): Operation[CV] = op match {
      case truth.loader.Insert(systemId, data) => Insert(systemId, data)
      case truth.loader.Update(systemId, oldData, newData) => Update(systemId, newData)(oldData)
      case truth.loader.Delete(systemId, oldData) => Delete(systemId)(oldData)
    }

    def convertEvent(ev: Delogger.LogEvent[CV]): Option[Event[CT, CV]] = ev match {
      case rdu: Delogger.RowDataUpdated[CV] =>
        Some(RowDataUpdated(rdu.operations.view.map(convertOp)))
      case Delogger.Truncated =>
        Some(Truncated)
      case Delogger.WorkingCopyDropped =>
        Some(WorkingCopyDropped)
      case Delogger.DataCopied =>
        Some(DataCopied)
      case Delogger.WorkingCopyPublished =>
        Some(WorkingCopyPublished)
      case Delogger.ColumnCreated(info) =>
        Some(ColumnCreated(makeSecondaryColumnInfo(info)))
      case Delogger.ColumnRemoved(info) =>
        Some(ColumnRemoved(makeSecondaryColumnInfo(info)))
      case Delogger.ComputationStrategyCreated(info) =>
        Some(ComputationStrategyCreated(makeSecondaryColumnInfo(info)))
      case Delogger.ComputationStrategyRemoved(info) =>
        Some(ComputationStrategyRemoved(makeSecondaryColumnInfo(info)))
      case Delogger.FieldNameUpdated(info) =>
        Some(FieldNameUpdated(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierSet(info) =>
        Some(RowIdentifierSet(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierCleared(info) =>
        Some(RowIdentifierCleared(makeSecondaryColumnInfo(info)))
      case Delogger.SystemRowIdentifierChanged(info) =>
        Some(SystemRowIdentifierChanged(makeSecondaryColumnInfo(info)))
      case Delogger.VersionColumnChanged(info) =>
        Some(VersionColumnChanged(makeSecondaryColumnInfo(info)))
      case Delogger.LastModifiedChanged(lastModified) =>
        Some(LastModifiedChanged(lastModified))
      case Delogger.WorkingCopyCreated(datasetInfo, copyInfo) =>
        Some(WorkingCopyCreated(makeSecondaryCopyInfo(copyInfo)))
      case Delogger.SnapshotDropped(info) =>
        Some(SnapshotDropped(makeSecondaryCopyInfo(info)))
      case Delogger.CounterUpdated(nextCounter) =>
        None
      case Delogger.RollupCreatedOrUpdated(info) =>
        Some(RollupCreatedOrUpdated(makeSecondaryRollupInfo(info)))
      case Delogger.RollupDropped(info) =>
        Some(RollupDropped(makeSecondaryRollupInfo(info)))
      case Delogger.RowsChangedPreview(inserted, updated, deleted, truncated) =>
        Some(RowsChangedPreview(inserted, updated, deleted, truncated))
      case Delogger.SecondaryReindex =>
        Some(SecondaryReindex)
      case Delogger.IndexDirectiveCreatedOrUpdated(info, directives) =>
        Some(IndexDirectiveCreatedOrUpdated(makeSecondaryColumnInfo(info), directives))
      case Delogger.IndexDirectiveDropped(info) =>
        Some(IndexDirectiveDropped(makeSecondaryColumnInfo(info)))
      case Delogger.IndexCreatedOrUpdated(info) =>
        Some(IndexCreatedOrUpdated(makeSecondaryIndexInfo(info)))
      case Delogger.IndexDropped(name) =>
        Some(IndexDropped(name))
      case Delogger.EndTransaction =>
        None
    }

    def saveMetrics(datasetInfo: metadata.DatasetInfo): Unit = {
      val datasetInternalName = makeSecondaryDatasetInfo(datasetInfo).internalName
      secondary.store.metric(datasetInternalName, currentCookie).foreach { metric =>
        u.secondaryMetrics.upsertDataset(secondary.storeId, datasetInfo.systemId, metric)
      }
    }

    def drop(): Unit = {
      timingReport("drop", "dataset" -> datasetId) {
        secondary.store.dropDataset(datasetIdFormatter(datasetId), currentCookie)
        dropFromSecondaryMap(deactivateAutoCommit)
      }
    }

    private def retrying[T](actions: => T, filter: Throwable => Unit): T = {
      try {
        return actions
      } catch {
        case e: Throwable =>
          logger.info("Rolling back to end transaction due to thrown exception:", e)
          u.rollback()
          // transaction isolation level is now reset to READ COMMITTED
          filter(e)
      }
      retrying[T](actions, filter)
    }

    /**
      * Serialize resync in the same store group
      * because resync will cause reads to be unavailable (at least in pg)
      */
    def resyncSerially(): Unit = {
      val sm = u.secondaryManifest
      try {
        // resync immediately calls commit before actual work is done.
        // That allows us to see resync in progress from the resync table.
        sm.lockResync(datasetId, secondary.storeId, secondary.groupName)
        resync()
      } finally {
        sm.unlockResync(datasetId, secondary.storeId, secondary.groupName)
      }
    }

    def dsInfoOf(datasetId: DatasetId) = {
      u.secondaryStoresConfig.lookup(secondary.storeId) match {
        case Some(config) if config.isFeedback =>
          logger.info("Non-exclusive resync")
          u.datasetMapReader.datasetInfo(datasetId, repeatableRead = true)
        case _ =>
          logger.info("exclusive resync")
          u.datasetMapWriter.datasetInfo(datasetId, datasetLockTimeout, semiExclusive = true)
      }
    }

    def resync(): Unit = {
      val mostRecentlyUpdatedCopyInfo = retrying[Option[metadata.CopyInfo]]({
        timingReport("resync", "dataset" -> datasetId) {
          u.commit() // all updates must be committed before we can change the transaction isolation level
          val r = u.datasetMapReader
          val w = u.datasetMapWriter
          dsInfoOf(datasetId) match {
            // transaction isolation level is now set to REPEATABLE READ or we have a lock on the dataset
            case Some(datasetInfo) =>
              val allCopies = r.allCopies(datasetInfo).toSeq.sortBy(_.dataVersion)
              val mostRecentCopy =
                if(allCopies.nonEmpty) {
                  val latestLiving = r.latest(datasetInfo) // this is the newest _living_ copy
                  val latestCopy = allCopies.maxBy(_.copyNumber)
                  for (copy <- allCopies) {
                    timingReport("copy", "number" -> copy.copyNumber) {
                      // secondary.store.resync(.) will be called
                      // on all _living_ copies in order by their copy number
                      def isDiscardedLike(stage: metadata.LifecycleStage) =
                        Set(metadata.LifecycleStage.Discarded, metadata.LifecycleStage.Snapshotted).contains(stage)
                      if (isDiscardedLike(copy.lifecycleStage)) {
                        val secondaryDatasetInfo = makeSecondaryDatasetInfo(copy.datasetInfo)
                        val secondaryCopyInfo = makeSecondaryCopyInfo(copy)
                        secondary.store.dropCopy(secondaryDatasetInfo, secondaryCopyInfo, currentCookie,
                          isLatestCopy = copy.copyNumber == latestCopy.copyNumber)
                      } else
                        syncCopy(copy, isLatestLivingCopy = copy.copyNumber == latestLiving.copyNumber)
                    }
                  }
                  Some(allCopies.last)
                } else {
                  logger.error("Have dataset info for dataset {}, but it has no copies?", datasetInfo.toString)
                  None
                }
              // end transaction to not provoke a serialization error from touching the secondary_manifest table
              u.commit()
              // transaction isolation level is now reset to READ COMMITTED
              mostRecentCopy
            case None =>
              drop()
              None
          }
        }
      }, {
        case ResyncSecondaryException(reason) =>
          logger.warn("Received resync while resyncing.  Resyncing as requested after waiting 10 seconds. " +
            " Reason: " + reason)
          Thread.sleep(10L * 1000)
        case e: Throwable => ignoreSerializationFailure(e)
      })
      retrying[Unit]({
        mostRecentlyUpdatedCopyInfo.foreach { case mostRecent =>
            timingReport("resync-update-secondary-map", "dataset" -> datasetId) {
              updateSecondaryMap(mostRecent.dataVersion)
            }
          }
      }, ignoreSerializationFailure)
    }

    private def ignoreSerializationFailure(e: Throwable): Unit = e match {
      case s: SQLException => extractPSQLException(s) match {
        case Some(p) =>
          if (p.getSQLState == "40001")
            logger.warn("Serialization failure occurred during REPEATABLE READ transaction: {}", p.getMessage)
          else
            throw s
        case None => throw s
      }
      case _ => throw e
    }

    private def extractPSQLException(s: SQLException): Option[PSQLException] = {
      var cause = s.getCause
      while(cause != null) cause match {
        case p: PSQLException => return Some(p)
        case _ => cause = cause.getCause
      }
      None
    }

    def syncCopy(copyInfo: metadata.CopyInfo, isLatestLivingCopy: Boolean): Unit = {
      timingReport("sync-copy",
                   "secondary" -> secondary.storeId,
                   "dataset" -> copyInfo.datasetInfo.systemId,
                   "copy" -> copyInfo.copyNumber) {
        for(reader <- u.datasetReader.openDataset(copyInfo)) {
          val copyCtx = new DatasetCopyContext(reader.copyInfo, reader.schema)
          val secondaryDatasetInfo = makeSecondaryDatasetInfo(copyCtx.datasetInfo)
          val secondaryCopyInfo = makeSecondaryCopyInfo(copyCtx.copyInfo)
          val secondarySchema = copyCtx.schema.mapValuesStrict(makeSecondaryColumnInfo)
          val itRows = reader.rows(sorted=false)
          // Sigh. itRows is a simple-arm v1 Managed.  v2 has a monad map() which makes the code below
          // much, much shorter.
          val wrappedRows = new Managed[Iterator[ColumnIdMap[CV]]] {
            def run[A](f: Iterator[ColumnIdMap[CV]] => A): A = {
              itRows.run { it: Iterator[ColumnIdMap[CV]] =>
                f(new InstrumentedIterator("sync-copy-throughput",
                                           copyInfo.datasetInfo.systemId.toString,
                                           it))
              }
            }
          }
          val rollups: Seq[RollupInfo] = u.datasetMapReader.rollups(copyInfo).toSeq.map(makeSecondaryRollupInfo)
          val indexes: Seq[IndexInfo] = u.datasetMapReader.indexes(copyInfo).toSeq.map(makeSecondaryIndexInfo)
          val indexDirectives = u.datasetMapReader.indexDirectives(copyInfo, None)

          currentCookie = secondary.store.resync(secondaryDatasetInfo,
                                                 secondaryCopyInfo,
                                                 secondarySchema,
                                                 currentCookie,
                                                 wrappedRows,
                                                 rollups,
                                                 indexDirectives,
                                                 indexes,
                                                 isLatestLivingCopy)
        }
      }
    }

    def updateSecondaryMap(newLastDataVersion: Long): Unit = {
      // We want to end the current transaction here. We don't want to be holding share locks on data-tables like log
      // tables while updating a row on the secondary_manifest. This is o avoid deadlocks when data-coordinator also has
      // locks out on the data-tables and is also updating the same row on the secondary_manifest.
      //
      // The activity in the current transaction (before committing) should all
      // be _reads_ from metadata tables and the dataset's log table.
      if(!u.isAutoCommit) u.commit()
      u.secondaryManifest.completedReplicationTo(secondary.storeId,
                                                 claimantId,
                                                 datasetId,
                                                 newLastDataVersion,
                                                 currentCookie)
      if(!u.isAutoCommit) u.commit()
    }

    def dropFromSecondaryMap(deactivateAutoCommit: DeactivateAutoCommit): Unit = {
      deactivateAutoCommit.close()
      u.secondaryMetrics.dropDataset(secondary.storeId, datasetId)
      u.secondaryManifest.dropDataset(secondary.storeId, datasetId)
      u.commit()
    }

    private class InternalResyncForPickySecondary extends ControlThrowable
  }
}
