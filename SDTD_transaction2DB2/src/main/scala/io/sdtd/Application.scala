package io.sdtd

import io.sdtd.configuration.KafkaFlinkConfiguration
import io.sdtd.processors.IncomingDataSplitter
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Application extends App {

  val kafkaConfig = KafkaFlinkConfiguration()

  IncomingDataSplitter.startDataProcess(StreamExecutionEnvironment.getExecutionEnvironment, kafkaConfig.getFlinkConsumer)
}
