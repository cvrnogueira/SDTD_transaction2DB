package io.sdtd.processors

import com.datastax.driver.core.{Cluster => CassandraCluster}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

class AsyncMetricsFetcher(host: List[String], port: Int) extends RichAsyncFunction[String, Int] {

  val session = new CassandraCluster.Builder()
    .addContactPoints(host: _*)
    .withPort(9142)
    .build()
    .connect()

  override def asyncInvoke(input: String, resultFuture: ResultFuture[Int]): Unit = {
    session
  }
}
