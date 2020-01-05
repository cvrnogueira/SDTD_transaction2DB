package io

import play.api.libs.json.{Json, Reads}

package object sdtd {

  implicit val readsTwitterPayload: Reads[TwitterPayload] = Json.using[Json.WithDefaultValues].format[TwitterPayload]
  implicit val readsWeatherPayload: Reads[WeatherPayload] = Json.using[Json.WithDefaultValues].format[WeatherPayload]

  case class TwitterPayload(
    tweet: String,
    createdAt: Long,
    location: String,
    counter: Long = 1,
    updatedAt: Long = 0
  )

  case class WeatherPayload(
    location: String,
    createdAt: Option[Long],
    aqi: Option[Long],
    severity: Option[String]
  )
}
