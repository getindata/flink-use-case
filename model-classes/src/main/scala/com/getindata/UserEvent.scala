package com.getindata

import java.time.temporal.ChronoField
import java.time.{Instant, ZoneId}

import com.getindata.DeviceType.DeviceType
import com.getindata.EventType.EventType
import com.getindata.PlaylistType.PlaylistType
import spray.json._

sealed trait Event extends Product {
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

object Event {

  import DefaultJsonProtocol._

  implicit val deviceTypeFormat = new RootJsonFormat[DeviceType] {
    override def read(json: JsValue): DeviceType = json match {
      case JsString(s) => DeviceType.withName(s)
      case _ => throw DeserializationException("Enum string expected")
    }

    override def write(obj: DeviceType): JsValue = JsString(obj.toString)
  }

  implicit val eventTypeFormat = new RootJsonFormat[EventType] {
    override def read(json: JsValue): EventType = json match {
      case JsString(s) => EventType.withName(s)
      case _ => throw DeserializationException("Enum string expected")
    }

    override def write(obj: EventType): JsValue = JsString(obj.toString)
  }

  implicit val playlistTypeFormat = new RootJsonFormat[PlaylistType] {
    override def read(json: JsValue): PlaylistType = json match {
      case JsString(s) => PlaylistType.withName(s)
      case _ => throw DeserializationException("Enum string expected")
    }

    override def write(obj: PlaylistType): JsValue = JsString(obj.toString)
  }

  implicit val songEventFormat = jsonFormat6(SongEvent)
  implicit val searchEventFormat = jsonFormat4(SearchEvent)

  implicit val eventFormat = new RootJsonFormat[Event] {

    override def read(json: JsValue): Event = json.asJsObject.getFields("type") match {
      case Seq(JsString("SongEvent")) => json.convertTo[SongEvent]
      case Seq(JsString("SearchEvent")) => json.convertTo[SearchEvent]
    }

    override def write(obj: Event): JsValue = JsObject((obj match {
      case s: SongEvent => s.toJson
      case s: SearchEvent => s.toJson
    }).asJsObject.fields + ("type" -> JsString(obj.productPrefix)))
  }

  implicit val userEventFormat = new RootJsonFormat[UserEvent] {

    override def read(json: JsValue): UserEvent = eventFormat.read(json).asInstanceOf[UserEvent]

    override def write(obj: UserEvent): JsValue = eventFormat.write(obj)
  }
}
