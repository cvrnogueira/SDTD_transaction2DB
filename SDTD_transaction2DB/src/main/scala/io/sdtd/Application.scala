package io.sdtd

import com.typesafe.scalalogging.LazyLogging
import io.sdtd.configuration.{DatabaseMigration, KafkaConfiguration}
import io.sdtd.processors._
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Application extends App with LazyLogging {

  logger.debug("initializing flink job")

  val cassandraHosts = List(sys.env.getOrElse("CASSANDRA_CLUSTER_ENTRY_POINT", "127.0.0.1"))

  val cassandraPort = 9042

  logger.debug(s"connecting to cassadra host ${cassandraHosts} and port ${cassandraPort}")

  DatabaseMigration.migrate(cassandraHosts, cassandraPort)

  val env = StreamExecutionEnvironment.getExecutionEnvironment.enableCheckpointing()

  val sourceStream = env.addSource(KafkaConfiguration.getTwitterKafkaConsumer)

  val asyncCounter = new AsyncCounterFetcher(cassandraHosts, cassandraPort)
  val twitterGroupedSource = TwitterWindowedGroupCount.pipeGrouped(sourceStream)
  val twitterGroupedEnrichedSource = TwitterMergeCount.pipe(twitterGroupedSource, asyncCounter)
  val twitterGroupedSink = TwitterStoreSink.pipeGrouped(cassandraHosts, twitterGroupedEnrichedSource)

  val twitterSingleSource = TwitterWindowedGroupCount.pipeSingle(sourceStream)
  val twitterSingleSink = TwitterStoreSink.pipeSingle(cassandraHosts, twitterSingleSource)

  val twitterTopMentions = TwitterStoreSink.pipeTop(cassandraHosts, twitterGroupedEnrichedSource)

  val weatherSourceSink = WeatherSink.pipe(env, KafkaConfiguration.getWeatherKafkaConsumer, cassandraHosts)

  env.execute(this.getClass.getName)
}
