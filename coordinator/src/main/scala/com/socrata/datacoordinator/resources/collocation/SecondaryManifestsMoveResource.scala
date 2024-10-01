package com.socrata.datacoordinator.resources.collocation

import java.io.IOException
import java.util.UUID

import com.rojoma.json.v3.io.JsonParseException
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import com.socrata.datacoordinator.external.CollocationError
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.secondary.SecondaryMoveJob
import com.socrata.datacoordinator.service.collocation._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}

case class SecondaryMoveJobsResult(moves: Seq[SecondaryMoveJob])

object SecondaryMoveJobsResult {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMoveJobsResult]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SecondaryMoveJobRequest(jobId: UUID, fromStoreId: String, toStoreId: String)

object SecondaryMoveJobRequest {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMoveJobRequest]
}

sealed abstract class InvalidMoveJob

case object StoreNotAcceptingDatasets extends InvalidMoveJob
case object StoreDisallowsCollocationMoveJob extends InvalidMoveJob
case object DatasetNotInStore extends InvalidMoveJob

case class SecondaryManifestsMoveResource(storeGroup: Option[String],
                                          datasetId: DatasetId,
                                          secondaryMoveJobs: (String, DatasetId) => Either[ResourceNotFound, SecondaryMoveJobsResult],
                                          ensureSecondaryMoveJob: (String, DatasetId, SecondaryMoveJobRequest) => Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]],
                                          rollbackSecondaryMoveJob: (DatasetId, SecondaryMoveJobRequest, Boolean) => Option[DatasetNotFound],
                                          datasetInternalName: DatasetId => DatasetInternalName)
  extends CollocationSodaResource {

  override def get = doGetSecondaryMoveJobs

  override def post = { req =>
    withBooleanParam("rollback", req) { rollback =>
      if (rollback) doRollbackMoveJob(req)
      else doMoveDataset(req)
    }
  }

  def doGetSecondaryMoveJobs(req: HttpRequest): HttpResponse = {
    storeGroup match {
      case Some(group) =>
        secondaryMoveJobs(group, datasetId) match {
          case Right(result) => responseOK(result)
          case Left(StoreGroupNotFound(_)) => storeGroupNotFound(group)
          case Left(DatasetNotFound(_)) => datasetNotFound(datasetInternalName(datasetId))
          case Left(error) =>
            log.error("Unexpected resource not found while doing move for dataset {}: {}", datasetId, error)
            InternalServerError
        }
      case None => storeGroupNotFound("")
    }
  }

  def doMoveDataset(req: HttpRequest): HttpResponse = {
    storeGroup match {
      case Some(group) =>
        withPostBody[SecondaryMoveJobRequest](req) { request =>
          ensureSecondaryMoveJob(group, datasetId, request) match {
            case Right(Right(datasetNewToStore)) => responseOK(datasetNewToStore)
            case Right(Left(StoreNotAcceptingDatasets)) =>
              errorResponse(
                BadRequest,
                CollocationError.STORE_NOT_ACCEPTING_NEW_DATASETS,
                "store" -> JString(request.toStoreId)
              )
            case Right(Left(StoreDisallowsCollocationMoveJob)) =>
              errorResponse(
                BadRequest,
                CollocationError.STORE_DOES_NOT_SUPPORT_COLLOCATION,
                "store" -> JString(request.toStoreId)
              )
            case Right(Left(DatasetNotInStore)) =>
              errorResponse(
                BadRequest,
                CollocationError.DATASET_NOT_FOUND_IN_STORE,
                "store" -> JString(request.fromStoreId)
              )
            case Left(StoreGroupNotFound(_)) => storeGroupNotFound(group)
            case Left(DatasetNotFound(_)) => datasetNotFound(datasetInternalName(datasetId))
            case Left(StoreNotFound(store)) => storeNotFound(store, BadRequest)
            case Left(error) =>
              log.error("Unexpected resource not found while doing move for dataset {}: {}", datasetId, error)
              InternalServerError
          }
        }
      case None => storeGroupNotFound("")
    }
  }

  def doRollbackMoveJob(req: HttpRequest): HttpResponse = {
    withBooleanParam("dropFromStore", req) { dropFromStore =>
      try {
        JsonUtil.readJson[SecondaryMoveJobRequest](req.servletRequest.getReader) match {
          case Right(request) =>
            rollbackSecondaryMoveJob(datasetId, request, dropFromStore) match {
              case None => responseOK(JObject.canonicalEmpty)
              case Some(DatasetNotFound(_)) => datasetNotFound(datasetInternalName(datasetId))
            }
          case Left(decodeError) =>
            log.warn("Unable to decode request: {}", decodeError.english)
            BadRequest ~> Json(JObject.canonicalEmpty)
        }
      } catch {
        case e: IOException =>
          log.error("Unexpected error while handling request", e)
          InternalServerError
        case e: JsonParseException =>
          log.warn("Unable to parse request as JSON", e)
          BadRequest ~> Json(JObject.canonicalEmpty)
      }
    }
  }
}
