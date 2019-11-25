package io.sdtd.configuration

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

case class KafkaFlinkConfiguration() {

  def getFlinkConsumer: FlinkKafkaConsumer010[String] = {

    val autoCategorizationConsumer = new FlinkKafkaConsumer010[String](
      "Transactions2DBTopicOut",
      new SimpleStringSchema(),
      getKafkaConsumerProps
    )

    autoCategorizationConsumer
  }

  private def getKafkaConsumerProps = {
    val properties = new Properties()

    properties.setProperty("group.id", "twitterTopicIn")
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("max.poll.records", "5")

    properties
  }
}
