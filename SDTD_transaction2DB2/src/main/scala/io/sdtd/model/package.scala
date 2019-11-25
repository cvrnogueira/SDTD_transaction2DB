package io

import play.api.libs.json.{Json, Reads}

package object sdtd {

  implicit val readsTwitterPayload: Reads[TwitterPayload] = Json.reads[TwitterPayload]

  case class TwitterPayload(
    userId: String,
    tweet: String,
    location: String
  )
}
