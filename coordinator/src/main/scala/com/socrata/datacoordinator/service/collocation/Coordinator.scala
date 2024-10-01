package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.io.JValueEventIterator
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode}
import com.rojoma.json.v3.util.AutomaticJsonDecodeBuilder
import com.socrata.datacoordinator.external.{CollocationError, SecondaryMetricsError}
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.resources.collocation._
import com.socrata.datacoordinator.resources.{SecondariesOfDatasetResult, SecondaryMetricsResult}
import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.http.client._
import com.socrata.datacoordinator.service.HostAndPort

case class CoordinatorError(errorCode: String, data: JObject)

object CoordinatorError {
  implicit val decode = AutomaticJsonDecodeBuilder[CoordinatorError]
}

sealed abstract class ErrorResult(message: String) extends Exception(message)

sealed abstract class RequestError(message: String) extends ErrorResult(message)

case class StoreDisallowsCollocation(store: String) extends RequestError(store)
case class ResponseError(error: DecodeError) extends RequestError(error.english)
case class UnexpectedError(message: String) extends RequestError(message)
case class UnexpectedCoordinatorError(errorCode: String) extends RequestError(s"Unexpected result code $errorCode from other coordinator")
case class InstanceNotFound(name: String) extends RequestError(s"Could not find host and port for data-coordinator.$name")

sealed abstract class ResourceNotFound(name: String, resource: String) extends ErrorResult(s"No such $resource $name")

case class StoreGroupNotFound(name: String) extends ResourceNotFound(name, "secondary store group")
case class StoreNotFound(name: String) extends ResourceNotFound(name, "secondary store")
case class DatasetNotFound(internalName: DatasetInternalName) extends ResourceNotFound(internalName.underlying, "dataset")

trait Coordinator {
  def defaultSecondaryGroups: Set[String]
  def secondaryGroupConfigs: Map[String, SecondaryGroupConfig]

  def secondaryGroups(storeGroup: String): Set[String] = storeGroup match {
    case "_DEFAULT_" => defaultSecondaryGroups
    case other => Set(other)
  }

  def collocatedDatasetsOnInstance(instance: String, datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  def dropCollocationsOnInstance(instance: String, internalName: DatasetInternalName): Option[ErrorResult]
  def secondariesOfDataset(internalName: DatasetInternalName): Either[RequestError, Option[SecondariesOfDatasetResult]]
  def ensureInSecondary(storeGroup: String, storeId: String, internalName: DatasetInternalName): Option[ErrorResult]
  def secondaryMetrics(storeId: String, instance: String): Either[ErrorResult, SecondaryMetric]
  def secondaryMetrics(storeId: String, internalName: DatasetInternalName): Either[ErrorResult, Option[SecondaryMetric]]
  def secondaryMoveJobs(instance: String, jobId: UUID): Either[RequestError, SecondaryMoveJobsResult]
  def secondaryMoveJobs(storeGroup: String, internalName: DatasetInternalName): Either[ErrorResult, SecondaryMoveJobsResult]
  def ensureSecondaryMoveJob(storeGroup: String, internalName: DatasetInternalName, request: SecondaryMoveJobRequest): Either[ErrorResult, Either[InvalidMoveJob, Boolean]]
  def rollbackSecondaryMoveJob(instance: String, jobId: UUID, move: Move, dropFromStore: Boolean): Option[ErrorResult]
}

abstract class CoordinatorProvider(val coordinator: Coordinator)

class HttpCoordinator(isThisInstance: String => Boolean,
                      val defaultSecondaryGroups: Set[String],
                      val secondaryGroupConfigs: Map[String, SecondaryGroupConfig],
                      hostAndPort: HostAndPort,
                      httpClient: HttpClient,
                      collocatedDatasets: Set[DatasetInternalName] => CollocatedDatasetsResult,
                      dropCollocations: DatasetInternalName => Unit,
                      secondariesOfDataset: DatasetId => Option[SecondariesOfDatasetResult],
                      ensureInSecondary: Coordinator => (String, String, DatasetId) => Boolean,
                      secondaryMetrics: (String, Option[DatasetId]) => Either[ResourceNotFound, Option[SecondaryMetric]],
                      secondaryMoveJobsByJob: UUID => SecondaryMoveJobsResult,
                      secondaryMoveJobs: (String, DatasetId) => Either[ResourceNotFound, SecondaryMoveJobsResult],
                      ensureSecondaryMoveJob: Coordinator => (String, DatasetId, SecondaryMoveJobRequest) => Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]],
                      rollbackSecondaryMoveJob: (DatasetId, SecondaryMoveJobRequest, Boolean) => Option[DatasetNotFound]) extends Coordinator {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpCoordinator])

  private def doRequest[T](instance: String, route: String, request: RequestBuilder => SimpleHttpRequest)
                          (handleResponse: Response => Either[RequestError, T]): Either[RequestError, T] = {
    hostAndPort(instance) match {
      case Some((host, port)) =>
        val builder = RequestBuilder(new java.net.URI(s"http://$host:$port$route"))

        httpClient.execute(request(builder)).run(handleResponse)
      case None =>
        log.warn("Unable to discover other instance {}", instance)
        Left(InstanceNotFound(instance))
    }
  }

  private def request[T : JsonDecode](instance: String, route: String)
                                     (request: RequestBuilder => SimpleHttpRequest): Either[RequestError, Option[T]] = {
    doRequest[Option[T]](instance, route, request) { response: Response =>
      response.resultCode match {
        case 200 | 201 =>
          response.value[T]() match {
            case Right(result) => Right(Some(result))
            case Left(error) =>
              log.error("Unable to parse response from other instance {} for status {}: {}",
                instance, response.resultCode.toString, error.english)
              Left(ResponseError(error))
          }
        case 404 => Right(None)
        case resultCode =>
          val message = s"Received unexpected result code $resultCode from other instance $instance!"
          log.error(message)
          Left(UnexpectedError(message))
      }
    }
  }

  private def noResponseRequest(instance: String, route: String, notFoundResult: ErrorResult)
                               (request: RequestBuilder => SimpleHttpRequest): Option[ErrorResult] = {
    doRequest[Option[ErrorResult]](instance, route, request) { response: Response =>
      response.resultCode match {
        case 200 => Right(None)
        case 404 => Right(Some(notFoundResult))
        case resultCode =>
          val message = s"Received unexpected result code $resultCode from other instance $instance!"
          log.error(message)
          Right(Some(UnexpectedError(message)))
      }
    } match {
      case Right(None) => None
      case Right(Some(error)) => Some(error)
      case Left(error) => Some(error)
    }
  }

  private def routeInternalName(route: String, internalName: DatasetInternalName): String =
    route + "/" + internalName.underlying

  private def routeInternalName(route: String, internalName: Option[DatasetInternalName]): String =
    route + internalName.map("/" + _.underlying).getOrElse("")

  private def collocationManifestRoute(instance: String, internalName: Option[DatasetInternalName] = None): String =
    routeInternalName(s"/collocation-manifest/$instance", internalName)

  override def collocatedDatasetsOnInstance(instance: String, datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult] = {
    if (isThisInstance(instance)) {
      Right(collocatedDatasets(datasets))
    } else {
      val route = collocationManifestRoute(instance)
      request[CollocatedDatasetsResult](instance, route)(_.jsonBody(datasets)) match {
        case Right(Some(result)) => Right(result)
        case Right(None) => Left(InstanceNotFound(instance))
        case Left(error) => Left(error)
      }
    }
  }

  override def dropCollocationsOnInstance(instance: String, internalName: DatasetInternalName): Option[ErrorResult] = {
    if (isThisInstance(instance)) {
      dropCollocations(internalName)
      None
    } else {
      val route = collocationManifestRoute(instance, Some(internalName))
      noResponseRequest(instance, route, InstanceNotFound(instance))(_.delete)
    }
  }

  override def secondariesOfDataset(internalName: DatasetInternalName): Either[RequestError, Option[SecondariesOfDatasetResult]] = {
    if (isThisInstance(internalName.instance)) {
      Right(secondariesOfDataset(internalName.datasetId))
    } else {
      val route = routeInternalName("/secondaries-of-dataset", internalName)
      request[SecondariesOfDatasetResult](internalName.instance, route)(_.get)
    }
  }

  override def ensureInSecondary(storeGroup: String, storeId: String, internalName: DatasetInternalName) =
    if(isThisInstance(internalName.instance)) {
      ensureInSecondary(this)(storeGroup, storeId, internalName.datasetId)
      None
    } else {
      val route = routeInternalName(s"/secondary-manifest/$storeId", internalName)
      noResponseRequest(internalName.instance, route, InstanceNotFound("Dataset " + internalName.underlying + " not found"))(_.json(JValueEventIterator(JObject.canonicalEmpty)))
    }

  private def secondaryMetricsRoute(storeId: String, internalName: Option[DatasetInternalName] = None) =
    routeInternalName(s"/secondary-manifest/metrics/$storeId", internalName)

  override def secondaryMetrics(storeId: String, instance: String): Either[ErrorResult, SecondaryMetric] = {
    if (isThisInstance(instance)) {
      secondaryMetrics(storeId, None) match {
        case Right(metric) => Right(metric.getOrElse(SecondaryMetric(0L)))
        case Left(error) => Left(error)
      }
    } else {
      request[SecondaryMetricsResult](instance, secondaryMetricsRoute(storeId))(_.get) match {
        case Right(Some(result)) => Right(SecondaryMetric(result.totalSizeBytes.getOrElse(0L)))
        case Right(None) => Left(StoreNotFound(storeId))
        case Left(requestError) => Left(requestError)
      }
    }
  }

  override def secondaryMetrics(storeId: String, internalName: DatasetInternalName): Either[ErrorResult, Option[SecondaryMetric]] = {
    if (isThisInstance(internalName.instance)) {
      secondaryMetrics(storeId, Some(internalName.datasetId))
    } else {
      doRequest[Either[ResourceNotFound, Option[SecondaryMetric]]](internalName.instance, secondaryMetricsRoute(storeId, Some(internalName)), _.get) { response =>
        response.resultCode match {
          case 200 => response.value[SecondaryMetricsResult]() match {
            case Right(result) => Right(Right(result.totalSizeBytes.map(SecondaryMetric)))
            case Left(error) => Left(ResponseError(error))
          }
          case 404 => response.value[CoordinatorError]() match {
            case Right(CoordinatorError(SecondaryMetricsError.STORE_DOES_NOT_EXIST, _)) =>
              Right(Left(StoreNotFound(storeId)))
            case Right(CoordinatorError(SecondaryMetricsError.DATASET_DOES_NOT_EXIST, _)) =>
              Right(Left(DatasetNotFound(internalName)))
            case Right(error) =>
              Left(UnexpectedError(s"Unexpected error of type ${error.getClass.getName}"))
            case Left(error) =>
              Left(ResponseError(error))
          }
          case resultCode =>
            val message = s"Received unexpected result code $resultCode from other instance ${internalName.instance}!"
            log.error(message)
            Left(UnexpectedError(message))
        }
      } match {
        case Right(Right(result)) => Right(result)
        case Right(Left(error)) => Left(error)
        case Left(error) => Left(error)
      }
    }
  }

  override def secondaryMoveJobs(instance: String, jobId: UUID): Either[RequestError, SecondaryMoveJobsResult] = {
    if (isThisInstance(instance)) {
      Right(secondaryMoveJobsByJob(jobId))
    } else {
      val route = s"/secondary-move-jobs/job/$jobId"
      request[SecondaryMoveJobsResult](instance, route)(_.get) match {
        case Right(result) => Right(result.get) // endpoint will never 404
        case Left(error) => Left(error)
      }
    }
  }

  private def secondaryMoveRoute(storeGroup: Option[String], internalName: DatasetInternalName) =
    routeInternalName("/secondary-manifest/move/" + storeGroup.getOrElse(""), internalName)

  override def secondaryMoveJobs(storeGroup: String, internalName: DatasetInternalName): Either[ErrorResult, SecondaryMoveJobsResult] = {
    if (isThisInstance(internalName.instance)) {
      secondaryMoveJobs(storeGroup, internalName.datasetId) match {
        case Right(result) => Right(result)
        case Left(resourceNotFound) => Left(resourceNotFound)
      }
    } else {
      request[SecondaryMoveJobsResult](internalName.instance, secondaryMoveRoute(Some(storeGroup), internalName))(_.get) match {
        case Right(Some(result)) => Right(result)
        case Right(None) => Left(DatasetNotFound(internalName))
        case Left(requestError) => Left(requestError)
      }
    }
  }

  override def ensureSecondaryMoveJob(storeGroup: String,
                                      internalName: DatasetInternalName,
                                      request: SecondaryMoveJobRequest): Either[ErrorResult, Either[InvalidMoveJob, Boolean]] = {
    if (secondaryGroupConfigs.contains(storeGroup)) {
      if (isThisInstance(internalName.instance)) {
        ensureSecondaryMoveJob(this)(storeGroup, internalName.datasetId, request)
      } else {
        val instance = internalName.instance
        doRequest[Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]]](instance, secondaryMoveRoute(Some(storeGroup), internalName), _.jsonBody(request)) { response =>
          response.resultCode match {
            case 200 => response.value[Boolean]() match {
              case Right(result) => Right(Right(Right(result)))
              case Left(error) => Left(ResponseError(error))
            }
            case 400 => response.value[CoordinatorError]() match {
              case Right(CoordinatorError(CollocationError.STORE_NOT_ACCEPTING_NEW_DATASETS, _)) =>
                Right(Right(Left(StoreNotAcceptingDatasets)))
              case Right(CoordinatorError(CollocationError.STORE_DOES_NOT_SUPPORT_COLLOCATION, _)) =>
                Right(Right(Left(StoreDisallowsCollocationMoveJob)))
              case Right(CoordinatorError(CollocationError.DATASET_NOT_FOUND_IN_STORE, _)) =>
                Right(Right(Left(DatasetNotInStore)))
              case Right(error) => Left(UnexpectedError(s"Unexpected error of type ${error.getClass.getName}"))
              case Left(error) => Left(ResponseError(error))
            }
            case 404 => response.value[CoordinatorError]() match {
              case Right(CoordinatorError(CollocationError.STORE_GROUP_DOES_NOT_EXIST, _)) =>
                Right(Left(StoreGroupNotFound(storeGroup)))
              case Right(CoordinatorError(CollocationError.DATASET_DOES_NOT_EXIST, _)) =>
                Right(Left(DatasetNotFound(internalName)))
              case Right(error) => Left(UnexpectedError(s"Unexpected error of type ${error.getClass.getName}"))
              case Left(error) => Left(ResponseError(error))
            }
            case resultCode =>
              val message = s"Received unexpected result code $resultCode from other instance $instance!"
              log.error(message)
              Left(UnexpectedError(message))
          }
        } match {
          case Right(Right(result)) => Right(result)
          case Right(Left(resourceNotFound)) => Left(resourceNotFound)
          case Left(requestError) => Left(requestError)
        }
      }
    } else {
      Left(StoreGroupNotFound(storeGroup))
    }
  }

  override def rollbackSecondaryMoveJob(instance: String, jobId: UUID, move: Move, dropFromStore: Boolean): Option[ErrorResult] = {
    val request = SecondaryMoveJobRequest(jobId, move.storeIdFrom, move.storeIdTo)
    if (isThisInstance(instance)) {
      rollbackSecondaryMoveJob(move.datasetInternalName.datasetId, request, dropFromStore)
    } else {
      val route = secondaryMoveRoute(None, move.datasetInternalName)
      noResponseRequest(instance, route, DatasetNotFound(move.datasetInternalName)) { req =>
        val parameters = Seq(
          ("rollback", "true"),
          ("dropFromStore", dropFromStore.toString)
        )
        req.addParameters(parameters).jsonBody(request)
      }
    }
  }
}
