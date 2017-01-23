package com.getindata

import java.time.Instant
import java.util.UUID

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class GeneratedEventsSource(private val maxInterval: Int) extends RichParallelSourceFunction[Event] {

  @volatile private var isRunning = true

  private val users = (1 to 10).map(_ => UUID.randomUUID().toString)
  private val songs = (1 to 20).map(_ => UUID.randomUUID().toString)

  override def cancel(): Unit = isRunning = false

  private def chooseEvent() = {
    val eventType = Random.nextDouble() match {
      case x if x > 0.5 => EventType.EndSong
      case x if x > 0.1 => EventType.NextSong
      case _ => EventType.SearchSong
    }

    val deviceType = Random.nextDouble() match {
      case x if x > 0.6 => DeviceType.Desktop
      case _ => DeviceType.Mobile
    }

    val userId = users(Random.nextInt(users.length))

    val timestamp = chooseTimestamp()

    eventType match {
      case EventType.EndSong =>
        val playlistType = Random.nextDouble() match {
          case x if x > 0.6 => PlaylistType.DiscoverWeekly
          case x if x > 0.2 => PlaylistType.DailyMix
          case _ => PlaylistType.ChillHits
        }
        val songId = songs(Random.nextInt(songs.length))
        SongEvent(timestamp, userId, eventType, deviceType, playlistType, songId)
      case _ => SearchEvent(timestamp, userId, eventType, deviceType)
    }

  }

  private def chooseTimestamp() = Instant.now.toEpochMilli

  override def run(ctx: SourceContext[Event]): Unit = {
    while (isRunning) {
      val event = chooseEvent()
      ctx.collect(event)
      Thread.sleep((Random.nextDouble() * maxInterval).toInt)
    }
  }
}
