import sbt._

object Dependencies {

  object Version {
    val kafka = "2.1.0"
    val scalaTest = "3.0.8"
    val twitter4j = "4.0.4"
    val cassandra = "3.6.0"
    val play = "2.6.10"
    val slf4j = "1.7.28"
    val log4j = "1.2.17"
    val flink = "1.6.1"
    val scalaLogging = "3.9.0"
    val cassandraDriver = "3.6.0"
  }

  val scalaTest = Seq("org.scalatest" %% "scalatest" % Version.scalaTest % Test)

  val playJSON = Seq("com.typesafe.play" %% "play-json" % Version.play)

  val logging = Seq(
    "org.slf4j" % "slf4j-api" % Version.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Version.slf4j,
    "log4j" % "log4j" % Version.log4j,
    "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  )

  val flink = Seq(
    "org.apache.flink" %% "flink-scala" % Version.flink,
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink,
    "org.apache.flink" %% "flink-clients" % Version.flink,
    "org.apache.flink" %% "flink-connector-kafka-0.10" % Version.flink,
    "org.apache.flink" %% "flink-connector-cassandra" % Version.flink
  )

  val all = scalaTest ++ logging ++ flink ++ playJSON
}