package com.getindata

import org.rogach.scallop.ScallopConf

class Conf(args: Array[String]) extends ScallopConf(args) {
  val topic = opt[String](required = false, descr = "Kafka topic to read (default: songs)", default = Some("songs"))
  val sessionWriteTopic = opt[String](required = false,
    descr = "Kafka topic to write (default: session-stats)",
    default = Some("session-stats"))
  val discoverWeeklyWriteTopic = opt[String](required = false,
    descr = "Kafka topic to write (default: discover-weekly-stats)",
    default = Some("discover-weekly-stats"))
  val subsessionWriteTopic = opt[String](required = false,
    descr = "Kafka topic to write (default: subsession-stats)",
    default = Some("subsession-stats"))
  val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")
  val sessionGap = opt[Int](required = false,
    descr = "Maximal session inactivity in seconds (default: 20)",
    default = Some(20))

  verify()

  def getTopic() = topic()

  def getKafkaBroker() = kafkaBroker()

  def getSessionGap() = sessionGap()

  def getSessionsWriteTopic() = sessionWriteTopic()

  def getDiscoverWeeklyWriteTopic() = discoverWeeklyWriteTopic()

  def getSubsessionWriteTopic: String = subsessionWriteTopic()
}
