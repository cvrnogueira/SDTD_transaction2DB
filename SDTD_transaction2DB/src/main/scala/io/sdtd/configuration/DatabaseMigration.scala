package io.sdtd.configuration

import com.datastax.driver.core.{Cluster => CassandraCluster}

object DatabaseMigration {

  val createKeyspaceQuery = "create keyspace if not exists sdtd with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

  val createTwitterTableQuery = "create table if not exists sdtd.twitterpayload (location varchar, location_sasi varchar, updatedat bigint, mentions bigint, primary key (location));"
  val createTwitterTableLocationSACIIndex = "create custom index if not exists sdtd_twitterpayload_location on sdtd.twitterpayload (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};"

  val createTwitterSingleTableQuery = "create table if not exists sdtd.twitterpayload_single (id uuid, location varchar, location_sasi varchar, createdat bigint, primary key (id));"
  val createTwitterSingleTableLocationSACIIndex = "create custom index if not exists sdtd_twitterpayload_location_single on sdtd.twitterpayload_single (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'true'};"

  val createWeatherTableQuery = "create table if not exists sdtd.weatherpayload (location varchar, location_sasi varchar, updatedat bigint, aqi bigint, severity varchar, primary key (location));"
  val createWeatherTableLocationSACIIndex = "create custom index if not exists sdtd_weatherpayload_location on sdtd.weatherpayload (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};"

  def migrate(hosts: List[String], port: Int) = {
    val session = new CassandraCluster.Builder()
      .addContactPoints(hosts: _*)
      .withPort(port)
      .build()
      .connect()

    session.execute(createKeyspaceQuery).wasApplied()
    session.execute(createTwitterTableQuery).wasApplied()
    session.execute(createTwitterTableLocationSACIIndex).wasApplied()
    session.execute(createTwitterSingleTableQuery).wasApplied()
    session.execute(createTwitterSingleTableLocationSACIIndex).wasApplied()
    session.execute(createWeatherTableQuery).wasApplied()
    session.execute(createWeatherTableLocationSACIIndex).wasApplied()

    session.close()
  }
}
