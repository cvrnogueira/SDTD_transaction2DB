package io.sdtd.configuration

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaConfiguration {

  val bootstrapHost = sys.env.getOrElse("KAFKA_CLUSTER_ENTRY_POINT", "127.0.0.1")

  def getWeatherKafkaConsumer: FlinkKafkaConsumer011[String] = {
    val properties = new Properties()

    properties.setProperty("group.id", "twitterTopicIn")
    properties.setProperty("bootstrap.servers", s"$bootstrapHost:9092")
    properties.setProperty("max.poll.records", "10")

    new FlinkKafkaConsumer011[String](
      "Weather2DBTopicOut",
      new SimpleStringSchema(),
      properties
    )
  }

  def getTwitterKafkaConsumer: FlinkKafkaConsumer011[String] = {
    val properties = new Properties()

    properties.setProperty("group.id", "twitterTopicIn")
    properties.setProperty("bootstrap.servers", s"$bootstrapHost:9092")
    properties.setProperty("enable.auto.commit", "false")

    new FlinkKafkaConsumer011[String](
      "Transactions2DBTopicOut",
      new SimpleStringSchema(),
      properties
    )
  }
}
