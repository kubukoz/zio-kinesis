package nl.vroste.zio.kinesis.client.dynamicconsumer

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.core.aspects.{ AwsCallAspect, Described }
import io.github.vigoo.zioaws.kinesis.{ model, Kinesis }
import io.github.vigoo.zioaws.kinesis.model.{ PutRecordsRequest, ScalingType, UpdateShardCountRequest }
import io.github.vigoo.zioaws.{ dynamodb, kinesis }
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.dynamicconsumer.ExampleApp.{ enhancedFanout, streamName }
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.random.Random
import zio.stream.{ ZStream, ZTransducer }

import java.nio.ByteBuffer

/**
 * Runnable used for manually testing various features
 */
object ExampleApp3 extends zio.App {
  val streamName                      = "mercury-invoice-generator-dev-stream-test"         // + java.util.UUID.randomUUID().toString
  val applicationName                 = "mercury-invoice-generator-kinesis-client-dev-test" // + java.util.UUID.randomUUID().toString(),
  val nrRecords                       = 10000
  val produceRate                     = 200                                                 // Nr records to produce per second
  val recordSize                      = 50
  val nrShards                        = 1
  val reshardFactor                   = 2
  val reshardAfter: Option[Duration]  = None                                                //Some(30.seconds)                                    // Some(10.seconds)
  val enhancedFanout                  = true
  val nrNativeWorkers                 = 0
  val nrKclWorkers                    = 1
  val runtime                         = 1.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 60 * 1000
  val recordProcessingTime: Duration  = 1.millisecond

  val producerSettings                = ProducerSettings(
    aggregate = false,
    metricsInterval = 5.seconds,
    bufferSize = 8192 * 8,
    maxParallelRequests = 10
  )

  val program: ZIO[Logging with Clock with Blocking with Random with Console with Kinesis with CloudWatch with Has[
    CloudWatchMetricsPublisherConfig
  ] with DynamicConsumer with LeaseRepository, Throwable, ExitCode] = {
    for {
      _              <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      _              <- TestUtil.getShards(streamName)
      producer       <- FooUtils.putRecordsEmitter(streamName, 200, nrRecords).mapError(_.toThrowable).runDrain
//      producer       <- TestUtil
//                    .produceRecords(streamName, nrRecords, produceRate, recordSize, producerSettings)
//                    .tapError(e => log.error(s"Producer error: ${e}"))
//                    .fork
//      _          <- producer.join
      ref            <- Ref.make(0)
      kclWorkers     <-
        ZIO.foreach((1 to nrKclWorkers))(id =>
          (for {
            shutdown <- Promise.make[Nothing, Unit]
            fib      <- kclWorker(ref, s"worker${id}", shutdown).forkDaemon
            _        <- ZIO.never.unit.ensuring(
                   log.warn(s"Requesting shutdown for worker worker${id}!") *> shutdown.succeed(()) <* fib.join.orDie
                 )
          } yield ()).fork
        )
      // Sleep, but abort early if one of our children dies
      _              <- reshardAfter
             .map(delay =>
               (log.info("Resharding") *>
                 kinesis.updateShardCount(
                   UpdateShardCountRequest(
                     streamName,
                     Math.ceil(nrShards.toDouble * reshardFactor).toInt,
                     ScalingType.UNIFORM_SCALING
                   )
                 ))
                 .delay(delay)
             )
             .getOrElse(ZIO.unit)
             .fork
      _              <- ZIO.sleep(runtime) raceFirst ZIO.foreachPar_(kclWorkers)(_.join) // raceFirst producer.join
      _               = println("Interrupting app")
//      _              <- producer.interruptFork
      _               = println("YYYYYYYYYYYYYYY 1")
      _              <- ZIO.foreachPar_(kclWorkers)(_.interrupt.map { exit =>
             exit.fold(_ => (), _ => println("kcl worker exiting"))
           })
      totalProcessed <- ref.get
      _               = println(s"XXXXXXXXXXXXXXXXXXXXXXXX total processed $totalProcessed")
    } yield ExitCode.success
  }
  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] =
    program
      .foldCauseM(e => log.error(s"Program failed: ${e.prettyPrint}", e).exitCode, ZIO.succeed(_))
      .provideCustomLayer(awsEnv)

  def kclWorker(
    ref: Ref[Int],
    id: String,
    requestShutdown: Promise[Nothing, Unit]
  ): ZIO[Random with Any with Blocking with Logging with Clock with DynamicConsumer, Throwable, Int] =
    for {
      d              <- zio.random.nextIntBetween(0, 1000)
      _              <- ZIO.sleep(d.millis)
      _              <- DynamicConsumer.consumeWith(
             streamName,
             applicationName = applicationName,
             deserializer = Serde.asciiString,
             isEnhancedFanOut = enhancedFanout,
             workerIdentifier = id,
             requestShutdown = requestShutdown.await,
             checkpointBatchSize = 200
           ) { _ =>
             ref.update(i => i + 1)
           //log.info(s"processing record ${rec}")
           }
      totalProcessed <- ref.get
    } yield totalProcessed

  def kclWorker2(
    id: String,
    requestShutdown: Promise[Nothing, Unit]
  ): ZIO[Logging with Blocking with Clock with DynamicConsumer with Any with Random, Throwable, Unit] =
    (ZStream.fromEffect(
      zio.random.nextIntBetween(0, 1000).flatMap(d => ZIO.sleep(d.millis))
    ) *> DynamicConsumer
      .shardedStream(
        streamName,
        applicationName = applicationName,
        deserializer = Serde.asciiString,
        isEnhancedFanOut = enhancedFanout,
        workerIdentifier = id,
        requestShutdown = requestShutdown.await
      )
      .flatMapPar(Int.MaxValue) {
        case (shardID, shardStream, checkpointer) =>
          shardStream
            .tap(r =>
              checkpointer
                .stageOnSuccess(log.info(s"${id} Processing record $r").when(false))(r)
            )
            .aggregateAsyncWithin(ZTransducer.collectAllN(1000), Schedule.fixed(5.minutes))
            .mapConcat(_.lastOption.toList)
            .tap(_ => log.info(s"${id} Checkpointing shard ${shardID}") *> checkpointer.checkpoint)
            .catchAll {
              case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                // Abort the stream when we no longer have the lease

                ZStream.fromEffect(log.error(s"${id} shard ${shardID} lost")) *> ZStream.empty
              case e                    =>
                ZStream.fromEffect(
                  log.error(s"${id} shard ${shardID} stream failed with" + e + ": " + e.getStackTrace)
                ) *> ZStream.fail(e)
            }
      }).runDrain

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val localStackEnv =
    LocalStackServices.localStackAwsLayer().orDie >+> (DynamoDbLeaseRepository.live) ++ loggingLayer

  val awsEnv: ZLayer[
    Any,
    Nothing,
    Kinesis with CloudWatch with dynamodb.DynamoDb with Logging with DynamicConsumer with LeaseRepository with Has[
      CloudWatchMetricsPublisherConfig
    ]
  ] = {
    val httpClient = HttpClientBuilder.make(
      maxConcurrency = 100,
      allowHttp2 = true,
      build = _.buildWithDefaults(
        AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
      )
    )

    val callLogging: AwsCallAspect[Logging] =
      new AwsCallAspect[Logging] {
        override final def apply[R1 <: Logging, A](
          f: ZIO[R1, AwsError, Described[A]]
        ): ZIO[R1, AwsError, Described[A]] =
          f.flatMap {
            case r @ Described(value @ _, description) =>
              log.info(s"Finished [${description.service}/${description.operation}]").as(r)
          }
      }

    val kinesisClient = kinesisAsyncClientLayer() @@ (callLogging)

    val cloudWatch = cloudWatchAsyncClientLayer()
    val dynamo     = dynamoDbAsyncClientLayer()
    val awsClients = ((ZLayer.requires[Logging] ++ httpClient) >+>
      io.github.vigoo.zioaws.core.config.default >>>
      (kinesisClient ++ cloudWatch ++ dynamo)).orDie

    val leaseRepo       = DynamoDbLeaseRepository.live
    val dynamicConsumer = DynamicConsumer.live
    val logging         = loggingLayer

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    logging >+> awsClients >+> (dynamicConsumer ++ leaseRepo ++ metricsPublisherConfig)
  }
}

object FooUtils {
  def makeRecord(i: Int, failIndex: Int = -1): String =
    if (i == failIndex)
      "DomainEventBad"
    else
      s"""{ "id": $i }"""

  def putRecordsEmitter(
    streamName: String,
    batchSize: Int,
    max: Int,
    failIndex: Int = -1
  ): ZStream[Console with Clock with Kinesis, AwsError, Int] =
    ZStream.service[Kinesis.Service].flatMap { client =>
      ZStream.unfoldM(0) { i =>
        if (i < max) {
          val recordsBatch =
            (i until i + batchSize)
              .map(batchIndex =>
                model.PutRecordsRequestEntry(
                  partitionKey = s"partitionKey$batchIndex",
                  data = Chunk.fromByteBuffer(ByteBuffer.wrap(makeRecord(batchIndex, failIndex).getBytes))
                )
              )
          val putRecordsM  = client
            .putRecords(PutRecordsRequest(records = recordsBatch, streamName))
          for {
            _ <- putStrLn(s"i=$i putting $batchSize  records into Kinesis")
            _ <- putRecordsM
            _ <- putStrLn(s"successfully put records")
          } yield Some((i, i + batchSize))
        } else
          ZIO.none
      }
    }

}
