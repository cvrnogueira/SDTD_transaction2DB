package io.sdtd.processors

import com.datastax.driver.mapping.Mapper
import io.sdtd.helpers.Helpers
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, MapperOptions}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

class TwitterProcessor {

  val CassandraSinkQuery = "INSERT INTO sdtd.twitterPayload (userId, tweet, location) values (?, ?, ?);"

  def process(env: StreamExecutionEnvironment, kafkaConsumer: FlinkKafkaConsumer010[String]): JobExecutionResult = {

    val source = env.addSource(kafkaConsumer)
      .map(s => Helpers.convertToTwitterPayload(s))
      .filter(_.isDefined)
      .map(_.get)

    CassandraSink.addSink(source)
      .setHost("127.0.0.1")
      .setQuery(CassandraSinkQuery)
      .setMapperOptions(new MapperOptions {
        override def getMapperOptions: Array[Mapper.Option] = Array(Mapper.Option.saveNullFields(true))
      })
      .build()

    env.execute("SDTDTransactions2DB")
  }
}
