package io.sdtd.processors

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object StoreSink {

  val CassandraSinkQuery = "UPDATE sdtd.twitterPayload SET createdAt = ?, counter = ? WHERE location = ?;"

  def pipe(host: List[String], source: DataStream[(Long, Long, String)]): CassandraSink[(Long, Long, String)] = {
    CassandraSink.addSink(source)
      .setHost(host.head)
      .setQuery(CassandraSinkQuery)
      .build()
  }
}
