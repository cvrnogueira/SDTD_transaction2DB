package io.sdtd.processors

import io.sdtd.TwitterPayload
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object TwitterStoreSink {

  val AggregateUpdateQuery = "update sdtd.twitterpayload set updatedat = dateof(now()), mentions = ?, location_sasi = ? where location = ?;"
  val SingleInsertQuery = "insert into sdtd.twitterpayload_single (id, location_sasi, createdat) values (uuid(), ?, ?);"

  def pipeGrouped(host: List[String], source: DataStream[TwitterPayload]): Unit = {

    val aggregatedSource = source.map { t => (t.counter, t.location.replaceAll("'", "").toLowerCase, t.location.replaceAll("'", "").toLowerCase) }

    CassandraSink.addSink(aggregatedSource)
      .setHost(host.head)
      .setQuery(AggregateUpdateQuery)
      .build()
  }

  def pipeSingle(host: List[String], source: DataStream[TwitterPayload]): Unit = {
    val singleSource = source.map { t => (t.location.replaceAll("'", "").toLowerCase, t.createdAt) }

    CassandraSink.addSink(singleSource)
      .setHost(host.head)
      .setQuery(SingleInsertQuery)
      .build()
  }
}
