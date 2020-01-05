package io.sdtd.configuration

import com.datastax.driver.core.{Cluster => CassandraCluster}

object DatabaseMigration {

  val createKeyspaceQuery = "create keyspace if not exists sdtd with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

  val createTwitterTableQuery = "create table if not exists sdtd.twitterpayload (location varchar, location_sasi varchar, updatedat bigint, mentions bigint, primary key (location));"
  val createTwitterTableLocationSACIIndex = "create custom index if not exists sdtd_twitterpayload_location on sdtd.twitterpayload (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};"

  val createTwitterSingleTableQuery = "create table if not exists sdtd.twitterpayload_single (updatedat bigint, mentions bigint, primary key (updatedat));"

  val createWeatherTableQuery = "create table if not exists sdtd.weatherpayload (location varchar, location_sasi varchar, updatedat bigint, aqi bigint, severity varchar, primary key (location));"
  val createWeatherTableLocationSACIIndex = "create custom index if not exists sdtd_weatherpayload_location on sdtd.weatherpayload (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};"

  val createTopTwitterMentionsTableQuery = "create table if not exists sdtd.top_twitter_mentions (now bigint, count bigint, location varchar, location_sasi varchar, primary key (now, count, location)) with clustering order by (count desc);"
  val createTopTwitterMentionsTableLocationSACIIndex = "create custom index if not exists top_twitter_mentions_sasi on sdtd.top_twitter_mentions (location_sasi) using 'org.apache.cassandra.index.sasi.SASIIndex' with options = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};"

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
    session.execute(createWeatherTableQuery).wasApplied()
    session.execute(createWeatherTableLocationSACIIndex).wasApplied()
    session.execute(createTopTwitterMentionsTableQuery).wasApplied()
    session.execute(createTopTwitterMentionsTableLocationSACIIndex).wasApplied()

    session.close()
  }
}
