package io

import java.util.UUID

import play.api.libs.json.{Json, Reads}

package object sdtd {

  implicit val readsTwitterPayload: Reads[TwitterPayload] = Json.using[Json.WithDefaultValues].format[TwitterPayload]

  case class TwitterPayload(
    tweet: String,
    createdAt: Long,
    location: String,
    counter: Long = 1
  )
}
