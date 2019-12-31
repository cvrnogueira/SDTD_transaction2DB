package io.sdtd.processors

import com.datastax.driver.core.{ResultSet, ResultSetFuture, Cluster => CassandraCluster}
import io.sdtd.TwitterPayload
import org.apache.flink.cassandra.shaded.com.google.common.util.concurrent.{FutureCallback, Futures}
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class AsyncCounterFetcher(host: List[String], port: Int) extends AsyncFunction[TwitterPayload, (TwitterPayload, Long)] {

  lazy val session = new CassandraCluster.Builder()
    .addContactPoints(host: _*)
    .withPort(port)
    .build()
    .connect()

  lazy val prepared = session.prepare("SELECT mentions FROM sdtd.twitterpayload where location = ?")

  implicit lazy val ex = Executors.directExecutionContext()

  override def asyncInvoke(input: TwitterPayload, resultFuture: ResultFuture[(TwitterPayload, Long)]): Unit = {
    session.executeAsync(prepared.bind(input.location))
      .map(_.asScala)
      .onComplete({
        case Success(r) => {
          val count = r.headOption.map(t => t.getLong("mentions")).getOrElse(0L)

          resultFuture.complete(List((input, count)))
        }

        case Failure(e) => resultFuture.completeExceptionally(e)
      })
  }

  implicit def cassandraFutureToScalaFuture(future: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]()

    val callback = new FutureCallback[ResultSet] {
      def onSuccess(result: ResultSet): Unit = {
        promise success result
      }

      def onFailure(err: Throwable): Unit = {
        promise failure err
      }
    }

    Futures.addCallback(future, callback)

    promise.future
  }
}
