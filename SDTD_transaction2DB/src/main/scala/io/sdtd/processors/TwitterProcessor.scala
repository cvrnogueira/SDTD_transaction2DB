package io.sdtd.processors

import io.sdtd.helpers.Helpers
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

class TwitterProcessor extends Serializable {

  private val EmptyLocationIdentifier = "Empty"
  private val CassandraSinkQuery = "UPDATE sdtd.twitterPayload SET createdAt = ?, counter = ? WHERE location = ?;"

  def process(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): JobExecutionResult = {

    val source = env.addSource(kafkaConsumer)
      .map(s => Helpers.convertToTwitterPayload(s))
      .filter(_.isDefined)
      .map(_.get)
      .filter(t => EmptyLocationIdentifier != t.location)
      .keyBy("location")
      .timeWindow(Time.seconds(5))
      .sum("counter")
      // convert to a tuple to make cassadrasink prepared statement
      // happy with args arity
      .map(t => (t.createdAt, t.counter, t.location))

    CassandraSink.addSink(source)
      .setHost(getCassandraHost())
      .setQuery(CassandraSinkQuery)
      .build()

    env.execute("SDTDTransactions2DB")
  }

  def getCassandraHost(): String = "127.0.0.1"
}
