package io.sdtd.processors

import io.sdtd.helpers.Helpers
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object WeatherSink {

  val CassandraSinkQuery = "update sdtd.weatherpayload set updatedat = dateof(now()), aqi = ?, severity = ?, location_sasi = ? where location = ?;"

  def pipe(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String], host: List[String]): CassandraSink[(Long, String, String, String)] = {
    val source: DataStream[(Long, String, String, String)] = env
      .addSource(kafkaConsumer)
      .map(s => Helpers.convertToWeatherPayload(s))
      .filter(_.isDefined)
      .map(_.get)
      .map(w => (w.aqi.getOrElse(-99), w.severity.getOrElse(""), w.location.replaceAll("'", "").toLowerCase, w.location.replaceAll("'", "").toLowerCase))

    CassandraSink.addSink(source)
      .setHost(host.head)
      .setQuery(CassandraSinkQuery)
      .build()
  }
}
