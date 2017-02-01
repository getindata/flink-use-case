package com.getindata.subsessions

import java.time.Duration

import com.getindata.{PlaylistType, SearchEvent, SongEvent, UserEvent}
import com.getindata.stats.{ContinousDiscoverWeekly, ContinousListening}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.Iterable

object Subsessions {
  @tailrec def mapToSubSession(l: Iterable[UserEvent],
                               splitPredicate: UserEvent => Boolean,
                               produceSubsession: Iterable[UserEvent] => Unit
                              ): Unit = {
    l.span(!splitPredicate(_)) match {
      case (Nil, Nil) =>
      case (x, Nil) => produceSubsession(x)
      case (Nil, (y :: ys)) =>
        mapToSubSession(ys, splitPredicate, produceSubsession)
      case (x, (y :: ys)) =>
        produceSubsession(x)
        mapToSubSession(ys, splitPredicate, produceSubsession)
    }

  }

  val countDiscoverWeekly: (String, TimeWindow, Iterable[UserEvent], Collector[ContinousDiscoverWeekly]) => Unit =
    (key, window, in, out) => {

      val produceSubsession: (Iterable[UserEvent]) => Unit = (events) => {
        val size = events.size
        val timestamps = events.map(_.timestamp)
        if (size > 0) {
          out.collect(ContinousDiscoverWeekly(key,
            Duration.ofMillis(timestamps.last - timestamps.head).getSeconds,
            size))
        }
      }

      val sorted = in.toList.sortBy(_.timestamp)
      mapToSubSession(sorted, {
        case _: SearchEvent => true
        case SongEvent(_, _, _, _, p, _) if p != PlaylistType.DiscoverWeekly => true
        case _ => false
      }, produceSubsession)
    }

  val countSubSessions: (String, TimeWindow, Iterable[UserEvent], Collector[ContinousListening]) => Unit =
    (key, window, in, out) => {

      val produceSubsession: (Iterable[UserEvent]) => Unit = (events) => {
        val size = events.size
        if (size > 0) {
          out.collect(ContinousListening(key, size))
        }
      }

      val sorted = in.toList.sortBy(_.timestamp)
      mapToSubSession(in, e => e.isInstanceOf[SearchEvent], produceSubsession)
    }
}
