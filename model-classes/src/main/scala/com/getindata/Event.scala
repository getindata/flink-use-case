package com.getindata

import com.getindata.DeviceType.DeviceType
import com.getindata.EventType.EventType
import com.getindata.PlaylistType.PlaylistType

sealed trait Event {
  val timestamp: Long
  val userId: String
  val eventType: EventType
  val deviceType: DeviceType
}

case class SongEvent(timestamp: Long,
                     userId: String,
                     eventType: EventType,
                     deviceType: DeviceType,
                     playlistType: PlaylistType,
                     songid: String) extends Event

case class SearchEvent(timestamp: Long,
                       userId: String,
                       eventType: EventType,
                       deviceType: DeviceType) extends Event
