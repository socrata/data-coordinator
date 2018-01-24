package com.socrata.datacoordinator.service.collocation

import com.socrata.datacoordinator.common.collocation.CollocationLock
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.resources.collocation.CollocatedDatasetsResult
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration.FiniteDuration

class CoordinatedCollocatorTest extends FunSuite with Matchers with MockFactory {
  class CollocationManifest {
    private val manifest = collection.mutable.Set.empty[(DatasetInternalName, DatasetInternalName)]

    def add(collocations: Seq[(DatasetInternalName, DatasetInternalName)]): Unit = {
      collocations.foreach(collocation => manifest.add(collocation))
    }

    def get = manifest.toSet
  }

  val alpha = "alpha"
  val bravo = "bravo"
  val charlie = "charlie"

  def expectNoMockCoordinatorCalls(mock: Coordinator): Unit = {
    (mock.rollbackSecondaryMoveJob _).expects(*, *, *, *).never
    (mock.ensureSecondaryMoveJob _).expects(*, *, *).never
    (mock.collocatedDatasetsOnInstance _).expects(*, *).never
    (mock.secondariesOfDataset _).expects(*).never
    (mock.secondaryMoveJobs _).expects(*, *).never
  }

  def expectCollocatedDatasetsOnInstance(mock: Coordinator,
                                         andNoOtherCalls: Boolean,
                                         params: (String, Set[DatasetInternalName],  Set[DatasetInternalName])*): Unit = {
    params.foreach { case (instance, datasets, result) =>
      (mock.collocatedDatasetsOnInstance _).expects(instance, datasets).once.returning(collocatedDatasets(result))
    }

    if (andNoOtherCalls) {
      (mock.rollbackSecondaryMoveJob _).expects(*, *, *, *).never
      (mock.ensureSecondaryMoveJob _).expects(*, *, *).never
      (mock.secondariesOfDataset _).expects(*).never
      (mock.secondaryMoveJobs _).expects(*, *).never
    }
  }

  def expectNoMockCollocationLockCalls(mock: CollocationLock): Unit = {
    (mock.release _).expects().never
    (mock.acquire _).expects(*).never
  }

  def withMocks[T](defaultStoreGroups: Set[String],
                        coordinatorExpectations: Coordinator => Unit = expectNoMockCoordinatorCalls,
                        collocationLockExpectations: CollocationLock => Unit = expectNoMockCollocationLockCalls)
                       (f: (Collocator, CollocationManifest) => T): T = {
    val manifest = new CollocationManifest

    val mockCoordinator: Coordinator = mock[Coordinator]
    val mockCollocationLock: CollocationLock = mock[CollocationLock]

    val collocator = new CoordinatedCollocator(
      collocationGroup = Set(alpha, bravo, charlie),
      defaultStoreGroups = defaultStoreGroups,
      coordinator = mockCoordinator,
      addCollocations = manifest.add,
      lock = mockCollocationLock,
      lockTimeoutMillis = FiniteDuration(10, "seconds").toMillis
    )

    coordinatorExpectations(mockCoordinator)
    collocationLockExpectations(mockCollocationLock)

    f(collocator, manifest)
  }

  val storeGroupA = "store_group_a"
  val storeGroupB = "store_group_b"

  val defaultStoreGroups = Set(storeGroupA, storeGroupB)

  def internalName(instance: String, datasetId: Long): DatasetInternalName =
    DatasetInternalName(instance, new DatasetId(datasetId))

  val alpha1 = internalName(alpha, 1L)
  val alpha2 = internalName(alpha, 2L)

  val bravo1 = internalName(bravo, 1L)
  val bravo2 = internalName(bravo, 2L)

  val charlie1 = internalName(charlie, 1L)
  val charlie2 = internalName(charlie, 2L)

  val datasetsEmpty = Set.empty[DatasetInternalName]

  def collocatedDatasets(datasets: Set[DatasetInternalName]) = Right(CollocatedDatasetsResult(datasets))

  val collocatedDatasetsEmpty = collocatedDatasets(datasetsEmpty)

  def expectCDOIDatasetsWithNoCollocations(coordinator: Coordinator,
                                           datasets: Set[DatasetInternalName],
                                           andNoOtherCalls: Boolean = false): Unit = {
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha, datasets, datasets),
      (bravo, datasets, datasets),
      (charlie, datasets, datasets)
    )
  }

  def expectCDOIAlpha1WithCollocationsSimple(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    // alpha.1 | bravo.1
    //
    // bravo
    // -----------------
    //
    // charlie
    // -----------------
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(alpha1), Set(alpha1, bravo1)),
      (bravo,   Set(alpha1), Set(alpha1)),
      (charlie, Set(alpha1), Set(alpha1)),

      (alpha,   Set(bravo1), Set(alpha1, bravo1)),
      (bravo,   Set(bravo1), Set(bravo1)),
      (charlie, Set(bravo1), Set(bravo1))
    )
  }

  def expectCDOIAlpha1WithCollocationsComplex(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    // alpha.1 | bravo.1
    //
    // bravo
    // -----------------
    // bravo.1 | alpha.2
    // bravo.1 | charlie.1
    //
    // charlie
    // -----------------
    // charlie.1 | alpha.2
    // charlie.2 | alpha.1
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(alpha1), Set(alpha1, bravo1)),
      (bravo,   Set(alpha1), Set(alpha1)),
      (charlie, Set(alpha1), Set(alpha1, charlie2)),
      // new: bravo1, charlie2

      (alpha,   Set(bravo1, charlie2), Set(alpha1, bravo1, charlie2)),
      (bravo,   Set(bravo1, charlie2), Set(bravo1, alpha2, charlie1, charlie2)),
      (charlie, Set(bravo1, charlie2), Set(alpha1, charlie2)),
      // new: alpha2, charlie1

      (alpha,   Set(alpha2, charlie1), Set(alpha2, charlie1)),
      (bravo,   Set(alpha2, charlie1), Set(bravo1, alpha2, charlie1)),
      (charlie, Set(alpha2, charlie1), Set(alpha2, charlie1))
    )
  }

  // tests for collocatedDatasets(datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  test("collocatedDatasets should return the empty set when given the empty set") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIDatasetsWithNoCollocations(coordinator, datasetsEmpty, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(datasetsEmpty) should be (collocatedDatasetsEmpty)
    }
  }

  test("collocatedDatasets for a dataset not collocated with any other should return just the dataset") {
    val datasets = Set(alpha1)

    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIDatasetsWithNoCollocations(coordinator, datasets, andNoOtherCalls = true)
    }) { case (collocator, _) =>
        collocator.collocatedDatasets(datasets) should be (collocatedDatasets(datasets))
    }
  }

  test("collocatedDatasets for a dataset collocated with one other dataset should return the pair") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIAlpha1WithCollocationsSimple(coordinator, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, bravo1)))
    }
  }

  test("collocatedDatasets for a dataset should be able to return its full collocated group of datasets") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIAlpha1WithCollocationsComplex(coordinator, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, alpha2, bravo1, charlie1, charlie2)))
    }
  }

  // test for defaultStoreGroups: Set[String]
  test("defaultStoreGroups should return the value CoordinatedCollocator was created with") {
    withMocks(defaultStoreGroups) { case (collocator, _) =>
        collocator.defaultStoreGroups should be (defaultStoreGroups)
    }
  }

  // def explainCollocation(storeGroup: String, request: CollocationRequest): Either[ErrorResult, CollocationResult]
  // def initiateCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)])
  // def saveCollocation(request: CollocationRequest): Unit
  // def beginCollocation(): Unit
  // def commitCollocation(): Unit
  // def rollbackCollocation(jobId: UUID, moves: Seq[(Move, Boolean)]): Option[ErrorResult]
}
