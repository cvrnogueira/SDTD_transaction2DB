package io.sdtd

import com.typesafe.scalalogging.LazyLogging
import io.sdtd.configuration.{DatabaseMigration, KafkaConfiguration}
import io.sdtd.processors._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Application extends App with LazyLogging {

  logger.debug("initializing flink job")

  val cassandraHosts = List(sys.env.getOrElse("CASSANDRA_CLUSTER_ENTRY_POINT", "127.0.0.1"))

  val cassandraPort = 9042

  logger.debug(s"connecting to cassadra host ${cassandraHosts} and port ${cassandraPort}")

  DatabaseMigration.migrate(cassandraHosts, cassandraPort)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val asyncCounter = new AsyncCounterFetcher(cassandraHosts, cassandraPort)

  val twitterSource = TwitterWindowedGroupCount.pipe(env, KafkaConfiguration.getTwitterKafkaConsumer)
  val twitterEnrichedSource = TwitterMergeCount.pipe(twitterSource, asyncCounter)
  val twitterSink = TwitterStoreSink.pipe(cassandraHosts, twitterEnrichedSource)

  val weatherSourceSink = WeatherSink.pipe(env, KafkaConfiguration.getWeatherKafkaConsumer, cassandraHosts)

  env.execute(this.getClass.getName)
}
