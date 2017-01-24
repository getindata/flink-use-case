package com.getindata

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoField

import com.getindata.DeviceType.DeviceType
import com.getindata.EventType.EventType
import com.getindata.PlaylistType.PlaylistType

sealed trait Event {
  val timestamp: Long

  def isWatermark: Boolean = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault())
    .get(ChronoField.SECOND_OF_MINUTE) < 2
}

sealed trait UserEvent extends Event {
  val userId: String
  val eventType: EventType
  val deviceType: DeviceType
}

case class SongEvent(timestamp: Long,
                     userId: String,
                     eventType: EventType,
                     deviceType: DeviceType,
                     playlistType: PlaylistType,
                     songid: String) extends UserEvent

case class SearchEvent(timestamp: Long,
                       userId: String,
                       eventType: EventType,
                       deviceType: DeviceType) extends UserEvent

