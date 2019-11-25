package io.sdtd.helpers

import com.datastax.driver.core.ConsistencyLevel
import io.sdtd.TwitterPayload
import io.sdtd.configuration.CassandraCluster


object CassandraStore extends CassandraCluster {

  // create document
  lazy val schemaCreateDocument =
    """
      |INSERT INTO cvrnogueira.twitterPayload(userId, createdAt, tweet, location)
      |VALUES(?, ?, ?, ?)
    """.stripMargin

  lazy val createDocumentStatement = session.prepare(schemaCreateDocument)
  createDocumentStatement.setConsistencyLevel(ConsistencyLevel.QUORUM)

  def createDocument(twitterPayload: TwitterPayload) = {

    val statement = createDocumentStatement.bind()
    statement.setString("userId", twitterPayload.userId)
    statement.setLong("createdAt", twitterPayload.createdAt)
    statement.setString("tweet", twitterPayload.tweet)
    // statement.setString("location", twitterPayload.location.getOrElse("None"))

    session.execute(statement)
  }

  // execute create document
  //val doc1 = TwitterPayload("1234",  1.asInstanceOf[Long], "test", Some("test"))
  //CassandraStore.createDocument(doc1)

  // get document
  lazy val schemaGetDocument =
    """
      |SELECT * FROM cvrnogueira.twitterPayload
      |WHERE userId = ?
      |LIMIT 1
    """.stripMargin
  lazy val getDocumentStatement = session.prepare(schemaGetDocument)

  def getDocument(userId: String): Option[TwitterPayload] = {

    val row = session.execute(getDocumentStatement.bind(userId)).one()
    if (row != null) {
      Option(
        TwitterPayload(
          row.getString("userId"),
          row.getLong("createdAt"),
          row.getString("tweet"),
          (row.getString("location"))
          // "EventTypeTeste"
        ))
    } else None
  }

  // execute get document
  val doc = CassandraStore.getDocument("1234")

}
