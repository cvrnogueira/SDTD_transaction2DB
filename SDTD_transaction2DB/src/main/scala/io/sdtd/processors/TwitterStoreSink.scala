package io.sdtd.processors

import java.time.LocalDate

import io.sdtd.TwitterPayload
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object TwitterStoreSink {

  val AggregateUpdateQuery = "update sdtd.twitterpayload set updatedat = dateof(now()), mentions = ?, location_sasi = ? where location = ?;"
  val SingleInsertQuery = "insert into sdtd.twitterpayload_single (mentions, updatedat) values (?, ?);"
  val TopMentionsInsertQuery = "insert into sdtd.top_twitter_mentions (now, location, location_sasi, count) values (?, ?, ?, ?);"

  def pipeTop(host: List[String], source: DataStream[TwitterPayload]): Unit = {
    val topSource = source.map { t =>
      val location = t.location.replaceAll("'", "").toLowerCase
      (today, location, location, t.counter)
    }

    CassandraSink.addSink(topSource)
      .setHost(host.head)
      .setQuery(TopMentionsInsertQuery)
      .build()
  }

  def pipeGrouped(host: List[String], source: DataStream[TwitterPayload]): Unit = {
    val aggregatedSource = source.map { t => (t.counter, t.location.replaceAll("'", "").toLowerCase, t.location.replaceAll("'", "").toLowerCase) }

    CassandraSink.addSink(aggregatedSource)
      .setHost(host.head)
      .setQuery(AggregateUpdateQuery)
      .build()
  }

  def pipeSingle(host: List[String], source: DataStream[(Long, Long)]): Unit = {
    CassandraSink.addSink(source)
      .setHost(host.head)
      .setQuery(SingleInsertQuery)
      .build()
  }

  @inline
  def today = LocalDate.now().toEpochDay
}
