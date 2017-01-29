package com.getindata

import java.time.{Duration, Instant}
import java.util.Properties

import com.getindata.serialization.JsonEventSerializationSchema
import com.getindata.triggers.WithEarlyTriggeringTrigger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.util.Collector
import org.rogach.scallop.ScallopConf

import scala.annotation.tailrec
import scala.collection.Iterable

object FlinkProcessingJob {

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val topic = opt[String](required = false, descr = "Kafka topic to read (default: songs)", default = Some("songs"))
    val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")
    val triggerInterval = opt[Int](required = false,
      descr = "Intervals in which to early trigger windows in seconds (default: 5).",
      default = Some(5))
    val sessionGap = opt[Int](required = false,
      descr = "Maximal session inactivity in seconds (default: 20)",
      default = Some(20))

    verify()
  }

  def kafkaProperties(bootstrapServer: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServer)
    properties
  }

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

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val kafkaConsumer = new FlinkKafkaConsumer09[Event](conf.topic(),
      getSerializationSchema,
      kafkaProperties(conf.kafkaBroker()))

    kafkaConsumer.assignTimestampsAndWatermarks(watermakAssigner)

    val kafkaEvents = env.addSource(kafkaConsumer).map(_.asInstanceOf[UserEvent]).name("Read events from Kafka")

    //#1 Count session length in seconds and songs

    kafkaEvents.map(_ match {
      case x: SongEvent => (x.userId, 1)
      case x: SearchEvent => (x.userId, 0)
    }).name("Change to tuple").keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
      .trigger(WithEarlyTriggeringTrigger.triggerEvery(Time.seconds(conf.triggerInterval())))
      .apply((e1, e2) => (e1._1, e1._2 + e2._2),
        (key, window, in, out: Collector[(String, Long, Long, Int)]) => {
          out.collect((key, window.getStart, window.getEnd, in.map(_._2).sum))
        })
      .name("Count sessions length")
      .map(e => s"User: ${e._1} session took ${
        Duration.ofMillis(e._3 - e._2).getSeconds
      } seconds and ${e._4} songs starting at ${Instant.ofEpochMilli(e._2)} and ending at " +
        s"${Instant.ofEpochMilli(e._3)}.")
      .print().name("Write to console")

    //#2 Count consecutive DiscoverWeekly session length in seconds and songs

    kafkaEvents.keyBy(_.userId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
      .apply(countDiscoverWeekly)
      .name("Count discover weekly subsessions statistics")
      .map(s =>
        s"""UserId ${s.userId} listened ${s.count} songs for ${s.length} seconds consecutively from Discover Weekly""")
      .print()

    //#3 Count songs until next or search clicked

    kafkaEvents.keyBy(_.userId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
      .apply(countSubSessions)
      .name("Count subsessions statistics")
      .map(s => s"UserId ${s.userId} listened ${s.count} songs consecutively")
      .print()


    env.execute("Session length")


  }

  private val watermakAssigner = new AssignerWithPunctuatedWatermarks[Event] {
    override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long) =
      if (lastElement.isWatermark) {new Watermark(extractedTimestamp)} else {null}

    override def extractTimestamp(element: Event, previousElementTimestamp: Long) = element.timestamp
  }

  private def getSerializationSchema = {
    new JsonEventSerializationSchema
  }

}
