package io.sdtd.helpers

import java.util.UUID

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
          Some(decodeResult.copy(userId = UUID.randomUUID().toString))
        }
        case _: JsError => {
          None
        }
      }
      case Failure(error) => logger.error(error.getMessage); None
    }
  }
}
