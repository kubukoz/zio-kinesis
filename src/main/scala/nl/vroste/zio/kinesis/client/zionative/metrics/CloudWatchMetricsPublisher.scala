package nl.vroste.zio.kinesis.client.zionative.metrics

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

import nl.vroste.zio.kinesis.client.Util.asZIO
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum

import scala.jdk.CollectionConverters._
import zio.Task
import zio.Queue
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import zio.stream.ZTransducer
import zio.Schedule
import zio.duration._

class CloudWatchMetricsPublisher(client: CloudWatchAsyncClient, eventQueue: Queue[DiagnosticEvent]) {
  val flushInterval: Duration = 20.seconds
  val maxBatchSize            = 20 // SDK limit
  val namespace: String       = "kinesis"

  def processEvent(e: DiagnosticEvent) = eventQueue.offer(e)

  def toMetrics(e: DiagnosticEvent): List[MetricDatum] = ???

  // TODO make sure queue is closed
  val processQueue =
    ZStream
      .fromQueue(eventQueue)
      .mapConcat(toMetrics)
      .aggregateAsyncWithin(ZTransducer.collectAllN(maxBatchSize), Schedule.fixed(flushInterval))
      .mapM(putMetricData)
      .runDrain

  private def putMetricData(metricData: Seq[MetricDatum]): Task[Unit] = {
    val request = PutMetricDataRequest
      .builder()
      .metricData(metricData.asJava)
      .namespace(namespace)
      .build()

    asZIO(client.putMetricData(request)).unit
  }
}
