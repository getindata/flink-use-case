package com.getindata.serialization

import java.nio.charset.StandardCharsets

import com.getindata.{Event, UserEvent}
import spray.json._

object EventCoderUtils {
  def encode(event: Event): Array[Byte] = event.toJson.toString().getBytes(StandardCharsets.UTF_8)

  def decodeEvent(array: Array[Byte]): Event = new String(array, StandardCharsets.UTF_8).parseJson.convertTo[Event]

  def encode(event: UserEvent): Array[Byte] = event.toJson.toString().getBytes(StandardCharsets.UTF_8)

  def decodeUserEvent(array: Array[Byte]): UserEvent = new String(array,
    StandardCharsets.UTF_8).parseJson.convertTo[UserEvent]
}
