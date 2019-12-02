package io.sdtd.configuration

import com.datastax.driver.core.{Cluster => CassandraCluster}

object DatabaseMigration {

  val createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS sdtd WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
  val createTableQuery = "CREATE TABLE IF NOT EXISTS sdtd.twitterPayload (location TEXT, createdAt bigint, counter bigint, PRIMARY KEY (location));"

  def migrate(hosts: List[String], port: Int) = {
    val session = new CassandraCluster.Builder()
      .addContactPoints(hosts: _*)
      .withPort(port)
      .build()
      .connect()

    session.execute(createKeyspaceQuery).wasApplied()
    session.execute(createTableQuery).wasApplied()

    session.close()
  }
}
