package io.sdtd.processors

import io.sdtd.helpers.Helpers
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object WeatherSink {

  val CassandraSinkQuery = "UPDATE sdtd.weatherPayload SET createdAt = ?, aqi = ?, severity = ? WHERE location = ?;"

  def pipe(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String], host: List[String]): CassandraSink[(Long, Long, String, String)] = {
    val source: DataStream[(Long, Long, String, String)] = env
      .addSource(kafkaConsumer)
      .map(s => Helpers.convertToWeatherPayload(s))
      .filter(_.isDefined)
      .map(_.get)
      .map(w => (w.createdAt.getOrElse(0), w.aqi.getOrElse(-99), w.severity.getOrElse(""), w.location))

    CassandraSink.addSink(source)
      .setHost(host.head)
      .setQuery(CassandraSinkQuery)
      .build()
  }
}
