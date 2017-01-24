package com.getindata

import org.apache.flink.util.Collector
import org.scalatest.{Matchers, WordSpec}

class FlinkProcessingJobTest extends WordSpec with Matchers {


  class CollectorMock extends Collector[ContinousListening] {

    var innerCollection: Seq[ContinousListening] = Seq()

    override def collect(record: ContinousListening): Unit = innerCollection :+= record

    override def close(): Unit = ???
  }

  "mapToSubSession" should {
    "not produce any results for only search events" in {
      val mock = new CollectorMock
      FlinkProcessingJob.mapToSubSession("1", mock, Seq(SearchEvent(1L, "1", EventType.NextSong, DeviceType.Desktop)))
      mock.innerCollection should be(Seq())
    }

    "produce one subsession" in {
      val mock = new CollectorMock
      FlinkProcessingJob.mapToSubSession("1", mock, Seq(
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "2"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "3"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "4"),
        SearchEvent(1L, "1", EventType.NextSong, DeviceType.Desktop)
      ))
      mock.innerCollection should be(Seq(ContinousListening("1", 3)))
    }

    "produce subsession when starts with next" in {
      val mock = new CollectorMock
      FlinkProcessingJob.mapToSubSession("1", mock, Seq(
        SearchEvent(1L, "1", EventType.NextSong, DeviceType.Desktop),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "2"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "3"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "4")
      ))
      mock.innerCollection should be(Seq(ContinousListening("1", 3)))
    }

    "produce multiple subsession" in {
      val mock = new CollectorMock
      FlinkProcessingJob.mapToSubSession("1", mock, Seq(
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "2"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "3"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "3"),
        SearchEvent(1L, "1", EventType.NextSong, DeviceType.Desktop),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "3"),
        SongEvent(1L, "1", EventType.EndSong, DeviceType.Desktop, PlaylistType.ChillHits, "4")
      ))
      mock.innerCollection should be(Seq(ContinousListening("1", 3), ContinousListening("1", 2)))
    }


  }

}
