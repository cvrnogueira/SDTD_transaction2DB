package io.sdtd

import io.sdtd.configuration.KafkaFlinkConfiguration
import io.sdtd.processors.{AsyncCounterFetcher, WindowedGroupCount, MergeCount, StoreSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Application extends App {

  // TODO: replace this hardcoded hosts list
  val cassandraHosts = List("127.0.0.1")

  // TODO: replace this hardcoded port
  val cassandraPort = 9042

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaConfig = KafkaFlinkConfiguration()
  val asyncCounter = new AsyncCounterFetcher(cassandraHosts, cassandraPort)

  val source = WindowedGroupCount.pipe(env, kafkaConfig.getFlinkConsumer)
  val enrichedSource = MergeCount.pipe(source, asyncCounter)
  val sink = StoreSink.pipe(cassandraHosts, enrichedSource)

  env.execute(this.getClass.getName)
}
