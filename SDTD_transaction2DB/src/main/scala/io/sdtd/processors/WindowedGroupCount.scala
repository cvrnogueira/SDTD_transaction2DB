package io.sdtd.processors

import io.sdtd.TwitterPayload
import io.sdtd.helpers.Helpers
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object WindowedGroupCount {

  val EmptyLocationIdentifier = "Empty"

  def pipe(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): DataStream[TwitterPayload] = {
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

      // keep a time window buffer with all tweets grouped by
      // location and once this window times count grouped length
      .keyBy("location")
      .timeWindow(Time.seconds(30))
      .sum("counter")
  }
}
