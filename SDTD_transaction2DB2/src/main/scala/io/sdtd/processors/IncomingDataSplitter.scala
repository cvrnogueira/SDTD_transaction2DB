package io.sdtd.processors

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object IncomingDataSplitter {

  def startDataProcess(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): JobExecutionResult = {
    new TwitterProcessor().process(env, kafkaConsumer)
  }
}
