package com.getindata

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.rogach.scallop.ScallopConf

import scala.annotation.tailrec
import scala.collection.Iterable
import scala.concurrent.duration.Duration

object FlinkProcessingJob {

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val topic = opt[String](required = false, descr = "Kafka topic to write", default = Some("songs"))
    val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")
    val sessionGap = opt[Int](required = false, descr = "Maximal session inactivity in minutes", default = Some(2))

    verify()
  }

  def kafkaProperties(bootstrapServer: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServer)
    properties
  }

  @tailrec def mapToSubSession(key: String,
                               out: Collector[ContinousListening],
                               l: Iterable[Event]): Unit = {
    val produceSubsession: (Iterable[Event]) => Unit = (events) => {
      val size = events.count(_.isInstanceOf[SongEvent])
      if (size > 0) {
        out.collect(ContinousListening(key, size))
      }
    }

    l.span(!_.isInstanceOf[SearchEvent]) match {
      case (x, y) if y.isEmpty => produceSubsession(x)
      case (x, (y :: ys)) =>
        produceSubsession(x)
        mapToSubSession(key, out, ys)
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val deserializationSchema = new TypeInformationSerializationSchema(
      TypeInformation.of(classOf[Event]), env.getConfig)

    val kafkaConsumer = new FlinkKafkaConsumer09[Event](conf.topic(),
      deserializationSchema,
      kafkaProperties(conf.kafkaBroker()))

    kafkaConsumer.assignTimestampsAndWatermarks(watermakAssigner)

    val kafkaEvents = env.addSource(kafkaConsumer).name("Read events from Kafka")

    kafkaEvents.map(_ match {
      case x: SongEvent => (x.userId, x.timestamp, x.timestamp, 1)
      case x: SearchEvent => (x.userId, x.timestamp, x.timestamp, 0)
    }).name("Change to tuple").keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
      .reduce((e1, e2) => (e1._1, math.min(e1._2, e2._2), math.max(e1._3, e2._3), e1._4 + e2._4))
      .name("Count sessions length")
      .map(e => s"User: ${e._1} session took ${
        Duration.create(e._3 - e._2,
          TimeUnit.MILLISECONDS).toSeconds
      } seconds and ${e._4} songs.").print().name("Write to console")

    val windowFunction: (String, TimeWindow, Iterable[Event], Collector[ContinousListening]) => Unit =
      (key, window, in, out) => {
        mapToSubSession(key, out, in)
      }

    kafkaEvents.keyBy(_.userId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(conf.sessionGap())))
      .apply(windowFunction)
      .name("Count subsessions statistics")
      .map(s => s"UserId ${s.userId} listened ${s.count} songs consecutively")
      .print()


    env.execute("Session length")


  }

  private val watermakAssigner = new AssignerWithPunctuatedWatermarks[Event] {
    override def checkAndGetNextWatermark(lastElement: Event,
                                          extractedTimestamp: Long) = new Watermark(extractedTimestamp)

    override def extractTimestamp(element: Event, previousElementTimestamp: Long) = element.timestamp
  }

}
