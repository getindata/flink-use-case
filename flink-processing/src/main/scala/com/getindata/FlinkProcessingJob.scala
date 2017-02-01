package com.getindata

import java.time.{Duration, Instant}
import java.util.Properties

import com.getindata.serialization.JsonEventSerializationSchema
import com.getindata.subsessions.Subsessions
import com.getindata.triggers.EarlyTriggeringTrigger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.util.Collector
import org.rogach.scallop.ScallopConf

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

    val discoverWeekly = toggle(default = Some(true), descrNo = "disable processing consecutive discover weekly events")
    val subsessionStats = toggle(default = Some(true), descrNo = "disable processing subsession stats")
    val sessionStats = toggle(default = Some(true), descrNo = "disable processing session stats")

    verify()
  }

  def kafkaProperties(bootstrapServer: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServer)
    properties
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

    if (conf.sessionStats()) {
      kafkaEvents.map(_ match {
        case x: SongEvent => (x.userId, 1)
        case x: SearchEvent => (x.userId, 0)
      }).name("Change to tuple").keyBy(_._1)
        .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
        .trigger(EarlyTriggeringTrigger.every(Time.seconds(conf.triggerInterval())))
        .apply((e1, e2) => (e1._1, e1._2 + e2._2),
          (key, window, in, out: Collector[(String, Long, Long, Int)]) => {
            out.collect((key, window.getStart, window.getEnd, in.map(_._2).sum))
          })
        .name("Count sessions length")
        .map(e =>
          s"""User: ${e._1} session took ${Duration.ofMillis(e._3 - e._2).getSeconds} seconds and ${e._4} songs
            starting at ${Instant.ofEpochMilli(e._2)} and ending at ${Instant.ofEpochMilli(e._3)}.""")
        .print().name("Write to console")
    }

    //#2 Count consecutive DiscoverWeekly session length in seconds and songs
    if (conf.discoverWeekly()) {
      kafkaEvents.keyBy(_.userId)
        .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
        .apply(Subsessions.countDiscoverWeekly)
        .name("Count discover weekly subsessions statistics")
        .map(s =>
          s"""UserId ${s.userId} listened ${s.count} songs for ${s.length} seconds consecutively from Discover
             |Weekly""".stripMargin)
        .print()
    }

    //#3 Count songs until next or search clicked
    if (conf.subsessionStats()) {
      kafkaEvents.keyBy(_.userId)
        .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
        .apply(Subsessions.countSubSessions)
        .name("Count subsessions statistics")
        .map(s => s"UserId ${s.userId} listened ${s.count} songs consecutively")
        .print()
    }

    env.execute("Session length")
  }

  private val watermakAssigner = new AssignerWithPunctuatedWatermarks[Event] {
    override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long): Watermark =
      if (lastElement.isWatermark) {new Watermark(extractedTimestamp)} else {null}

    override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = element.timestamp
  }

  private def getSerializationSchema = {
    new JsonEventSerializationSchema
  }

}
