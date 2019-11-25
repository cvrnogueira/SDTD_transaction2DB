package io.sdtd.helpers

import com.typesafe.scalalogging.StrictLogging
import io.sdtd.TwitterPayload
import play.api.libs.json.{JsError, JsPath, JsSuccess, Json}

import scala.util.{Failure, Success, Try}

object Helpers extends Serializable with StrictLogging {

  def convertToTwitterPayload(message: String): Option[TwitterPayload] = {
    Try(Json.parse(message)) match {
      case Success(json) => Json.fromJson[TwitterPayload](json)
      match {
        case JsSuccess(decodeResult: TwitterPayload, _: JsPath) => {
          Some(decodeResult)
        }
        case e: JsError => {
          print(e)
          None
        }
      }
      case Failure(error) => logger.error(error.getMessage); None
    }
  }
}
