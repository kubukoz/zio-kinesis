package nl.vroste.zio.kinesis.client.zionative

import zio.test._
import zio.test.Gen

import zio.test.DefaultRunnableSpec
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.Lease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator
import zio.logging.slf4j.Slf4jLogger
// import zio.UIO
import zio.random.Random

object LeaseStealingTest extends DefaultRunnableSpec {
  val leaseDistributionGen = leases(Gen.int(2, 100), Gen.int(2, 10))

  def workerId(w: Int): String = s"worker-${w}"

  def leases(nrShards: Gen[Random, Int], nrWorkers: Gen[Random, Int], allOwned: Boolean = true) =
    for {
      nrShards    <- nrShards
      nrWorkers   <- nrWorkers
      randomWorker = Gen.int(1, nrWorkers)
      leases      <- genTraverse(0 until nrShards) { shard =>
                  for {
                    worker     <-
                      (if (allOwned) randomWorker.map(Some(_)) else Gen.option(randomWorker)).map(_.map(workerId))
                    counter    <- Gen.long(1, 1000)
                    sequenceNr <-
                      Gen.option(Gen.int(0, Int.MaxValue / 2).map(_.toString).map(ExtendedSequenceNumber(_, 0L)))
                  } yield Lease(s"shard-${shard}", worker, counter, 0L, sequenceNr, Seq.empty, None)
                }
    } yield leases

  import zio.test.Assertion._
  import DynamoDbLeaseCoordinator.leasesToSteal
  // import TestAspect.ignore

  override def spec =
    suite("Lease coordinator")(
      testM("does not want to steal leases if its the only worker") {
        checkM(leases(nrShards = Gen.int(2, 100), nrWorkers = Gen.const(1))) { leases =>
          assertM(leasesToSteal(leases, workerId(1)))(isEmpty)
        }
      }, // @@ TestAspect.ignore,
      testM("steals some leases when its not the only worker") {
        checkM(leases(nrShards = Gen.int(2, 100), nrWorkers = Gen.const(1))) { leases =>
          assertM(leasesToSteal(leases, workerId(2)))(isNonEmpty)
        }
      }, // @@ TestAspect.ignore,
      testM("steals leases if it has less than its equal share") {
        checkM(leases(nrShards = Gen.int(2, 100), nrWorkers = Gen.int(1, 10), allOwned = true)) {
          leases =>
            // {
            // val leases = List(Lease("shard-0",Some("worker-2"),1,0,None,List(),None), Lease("shard-1",Some("worker-2"),1,0,None,List(),None))
            val workers       = leases.map(_.owner).collect { case Some(owner) => owner }.toSet
            val nrWorkers     = (workers + workerId(1)).size
            val nrOwnedLeases = leases.map(_.owner).collect { case Some(owner) if owner == workerId(1) => 1 }.size
            val expectedShare = Math.floor(leases.size * 1.0 / nrWorkers).toInt
            println(
              s"For ${leases.size} leases, ${nrWorkers} workers, nr owned leases ${nrOwnedLeases}: expecting share ${expectedShare}"
            )

            val expectedToSteal = Math.max(0, expectedShare - nrOwnedLeases) // We could own more than our fair share

            for {
              toSteal <- DynamoDbLeaseCoordinator.leasesToSteal(leases, workerId(1))
            } yield assert(toSteal.size)(equalTo(expectedToSteal))
        }
      }, //  @@ TestAspect.ignore,
      testM("steals from the busiest workers first") {
        checkM(leases(nrShards = Gen.int(2, 100), nrWorkers = Gen.int(1, 10))) {
          leases =>
            println(s"Beginning check with ${leases.size} leases")
            val leasesByWorker = leases
              .groupBy(_.owner)
              .collect { case (Some(owner), leases) => owner -> leases }
              .toList
              .sortBy { case (worker, leases) => (leases.size * -1, worker) }
            val busiestWorkers = leasesByWorker.map(_._1)

            for {
              toSteal       <- DynamoDbLeaseCoordinator.leasesToSteal(leases, workerId(1))
              // The order of workers should be equal to the order of busiest workers
              toStealWorkers = changedElements(toSteal.map(_.owner.get))
            } yield assert(toStealWorkers)(equalTo(busiestWorkers.take(toStealWorkers.size)))
        }
      }
    ).provideCustomLayer(loggingEnv)

  def changedElements[A](as: List[A]): List[A] =
    as.foldLeft(List.empty[A]) { case (acc, a) => if (acc.lastOption.contains(a)) acc else acc :+ a }

  val loggingEnv                               = Slf4jLogger.make((_, logEntry) => logEntry, Some("NativeConsumerTest"))

  def genTraverse[R, A, B](elems: Iterable[A])(f: A => Gen[R, B]): Gen[R, List[B]] =
    Gen.crossAll(elems.map(f))
}