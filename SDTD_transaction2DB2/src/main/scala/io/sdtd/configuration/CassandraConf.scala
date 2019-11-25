package io.sdtd.configuration

import akka.event.slf4j.SLF4JLogging
import com.datastax.driver.core.{Cluster, _}

trait CassandraCluster extends SLF4JLogging {

  lazy val cassandraKeyspace = "sdtd"
  lazy val cassandraHosts = List("localhost")
  lazy val cassandraPort = 9042

  lazy val poolingOptions: PoolingOptions = {
    new PoolingOptions()
      .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
      .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)
  }

  lazy val cluster: Cluster = {
    val builder = Cluster.builder()
    for (cp <- cassandraHosts) builder.addContactPoint(cp)
    builder.withPort(cassandraPort)
    builder.withPoolingOptions(poolingOptions)
    builder.build()
  }

  lazy implicit val session: Session = cluster.connect()
}