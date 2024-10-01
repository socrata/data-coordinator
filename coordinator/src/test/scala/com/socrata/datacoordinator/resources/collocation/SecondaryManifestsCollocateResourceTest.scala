package com.socrata.datacoordinator.resources.collocation

import java.util.UUID

import com.socrata.datacoordinator.collocation.TestData.{request, _}
import com.socrata.datacoordinator.service.collocation._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

class
SecondaryManifestsCollocateResourceTest extends FunSuite with Matchers with MockFactory {
  def withMock(storeGroup: String,
               explain: Boolean = false)
              (collocatorExpectations: Collocator => Unit)(f: (SecondaryManifestsCollocateResource, UUID) => Unit): Unit = {
    val mockCollocator = mock[Collocator]

    val mockProvider = new CoordinatorProvider(mock[Coordinator]) with CollocatorProvider {
      override val collocator = mockCollocator
    }
    collocatorExpectations(mockCollocator)

    f(SecondaryManifestsCollocateResource(storeGroup, mockProvider), UUID.randomUUID())
  }

  test("Should return the approved collocation result for the secondary groups on explain") {
    val jobId = UUID.randomUUID()
    val movesA = Seq(
      Move(bravo1, store3A, store1A, Cost(1, 10)),
      Move(bravo1, store5A, store7A, Cost(1, 10))
    )
    val movesB = Seq(
      Move(bravo1, store2B, store4B, Cost(1, 10)),
      Move(bravo1, store6B, store8B, Cost(1, 10))
    )
    val resultA = Right(CollocationResult(None, Approved, Approved.message, Cost(2, 20, Some(10)), movesA))
    val resultB = Right(CollocationResult(None, Approved, Approved.message, Cost(2, 20, Some(10)), movesB))

    val expectedResult = Right(CollocationResult(None, Approved, Approved.message, Cost(4, 40, Some(10)), movesA ++ movesB))

    withMock("_DEFAULT_", explain = true) { collocator =>
      (collocator.explainCollocation _).expects(storeGroupA, request(Seq((alpha1, bravo1)))).once.returns(resultA)
      (collocator.explainCollocation _).expects(storeGroupB, request(Seq((alpha1, bravo1)))).once.returns(resultB)

    } { (resource, _) =>
      val result = resource.doCollocationJob(jobId, Set(storeGroupA, storeGroupB), request(Seq((alpha1, bravo1))), explain = true)
      result should be (expectedResult)
    }
  }

  test("Should return the in-progress collocation result for the secondary groups on execute") {
    val collocationRequest = request(Seq((alpha1, bravo1)))
    val jobId = UUID.randomUUID()
    val movesA = Seq(
      Move(bravo1, store3A, store1A, Cost(1, 10)),
      Move(bravo1, store5A, store7A, Cost(1, 10))
    )
    val movesB = Seq(
      Move(bravo1, store2B, store4B, Cost(1, 10)),
      Move(bravo1, store6B, store8B, Cost(1, 10))
    )
    val resultA = Right(CollocationResult(Some(jobId), InProgress, InProgress.message, Cost(2, 20, Some(10)), movesA))
    val resultB = Right(CollocationResult(Some(jobId), InProgress, InProgress.message, Cost(2, 20, Some(10)), movesB))

    val expectedResult = Right(CollocationResult(Some(jobId), InProgress, InProgress.message, Cost(4, 40, Some(10)), movesA ++ movesB))

    withMock(storeGroupA, explain = true) { collocator =>

      (collocator.executeCollocation _).expects(jobId, storeGroupA, request(Seq((alpha1, bravo1)))).once.returns((resultA, movesA.map((_, true))))
      (collocator.executeCollocation _).expects(jobId, storeGroupB, request(Seq((alpha1, bravo1)))).once.returns((resultB, movesB.map((_, true))))
      (collocator.commitCollocation _).expects(jobId, collocationRequest)

    } { (resource, _) =>
      val result = resource.doCollocationJob(jobId, Set(storeGroupA, storeGroupB), collocationRequest, explain = false)
      result should be (expectedResult)
    }
  }
}
