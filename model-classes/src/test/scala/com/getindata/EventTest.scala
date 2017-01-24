package com.getindata

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

import org.scalatest.{Matchers, WordSpec}

class EventTest extends WordSpec with Matchers {
  "watermark" should {
    "be every event within 2 sec after full minute" in {
      val event = SearchEvent(
        ZonedDateTime.of(LocalDate.now, LocalTime.of(1, 1, 1), ZoneId.systemDefault()).toInstant.toEpochMilli,
        "",
        EventType.NextSong,
        DeviceType.Desktop
      )

      event.isWatermark should be (true)
    }

    "should not marked for events 2 or more sec after full minute" in {
      val event = SearchEvent(
        ZonedDateTime.of(LocalDate.now, LocalTime.of(1, 1, 2), ZoneId.systemDefault()).toInstant.toEpochMilli,
        "",
        EventType.NextSong,
        DeviceType.Desktop
      )

      event.isWatermark should be (false)
    }
  }
}
