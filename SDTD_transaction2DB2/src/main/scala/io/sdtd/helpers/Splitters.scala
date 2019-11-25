package io.sdtd.helpers

import com.typesafe.scalalogging.StrictLogging

object Splitters extends Serializable with StrictLogging {

  /*
    def splitterByEventType: TwitterPayload => Seq[String]  = {
      def processFailure(eventType: String) = {
        logger.error(s"Could not split given the incoming data: $eventType")
        Seq("DefaultErrorLabel")
      }

      results: TwitterPayload => {
        if (results.eventType.equals("twitterEvent")) {
          Seq("Twitter")
        }
        else  if (results.eventType.equals("weatherEvent")) {
          Seq("Weather")
        }
        else {
          processFailure(results.eventType)
        }
      }
    }
  */
}
