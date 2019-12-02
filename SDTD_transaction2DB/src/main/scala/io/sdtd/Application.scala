package io.sdtd

import io.sdtd.configuration.{DatabaseMigration, KafkaFlinkConfiguration}
import io.sdtd.processors.{AsyncCounterFetcher, MergeCount, StoreSink, WindowedGroupCount}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Application extends App {

  val cassandraHosts = List(sys.env.getOrElse("CASSANDRA_CLUSTER_ENTRY_POINT", "127.0.0.1"))

  val cassandraPort = 9042

  DatabaseMigration.migrate(cassandraHosts, cassandraPort)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaConfig = KafkaFlinkConfiguration()
  val asyncCounter = new AsyncCounterFetcher(cassandraHosts, cassandraPort)

  val source = WindowedGroupCount.pipe(env, kafkaConfig.getFlinkConsumer)
  val enrichedSource = MergeCount.pipe(source, asyncCounter)
  val sink = StoreSink.pipe(cassandraHosts, enrichedSource)

  env.execute(this.getClass.getName)
}
