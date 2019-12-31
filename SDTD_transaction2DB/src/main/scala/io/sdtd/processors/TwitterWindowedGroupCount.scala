package io.sdtd.processors

import io.sdtd.TwitterPayload
import io.sdtd.helpers.Helpers
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object TwitterWindowedGroupCount {

  val EmptyLocationIdentifier = "Empty"

  def filter(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): DataStream[TwitterPayload] = {
    env
      // consumes and convert string tweet to
      // a tweet object
      .addSource(kafkaConsumer)
      .map(s => Helpers.convertToTwitterPayload(s))

      // tweets can have no location at all or
      // custom locations then we filter non-empty location
      // tweets to be processed
      .filter(_.isDefined)
      .map(_.get)
      .filter(t => EmptyLocationIdentifier != t.location)
  }

  def pipeGrouped(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): DataStream[TwitterPayload] = {
    filter(env, kafkaConsumer)

      // keep a time window buffer with all tweets grouped by
      // location and once this window times count grouped length
      .keyBy("location")
      .timeWindow(Time.seconds(5))
      .sum("counter")
  }

  @inline
  def pipeSingle(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): DataStream[TwitterPayload] = filter(env, kafkaConsumer)
}
