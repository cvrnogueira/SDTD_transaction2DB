package io.sdtd.processors

import java.util.concurrent.TimeUnit

import io.sdtd.TwitterPayload
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}

object TwitterMergeCount {

  def pipe(source: DataStream[TwitterPayload], asyncFn: AsyncFunction[TwitterPayload, (TwitterPayload, Long)]): DataStream[TwitterPayload] = {

    AsyncDataStream.unorderedWait(
      input = source,
      asyncFunction = asyncFn,
      timeout = 6,
      timeUnit = TimeUnit.SECONDS,
      capacity = 100
    )

    // convert to a tuple to make cassadrasink prepared statement
    // happy with args arity
    .map(t => t._1.copy(counter =  t._1.counter + t._2))
  }
}
