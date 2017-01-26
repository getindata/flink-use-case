package com.getindata

import org.rogach.scallop.ScallopConf

class Conf(args: Array[String]) extends ScallopConf(args) {
  val topic = opt[String](required = false, descr = "Kafka topic to read", default = Some("songs"))
  val writeTopic = opt[String](required = false, descr = "Kafka topic to write", default = Some("session-stats"))
  val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")
  val sessionGap = opt[Int](required = false, descr = "Maximal session inactivity in minutes", default = Some(2))

  verify()

  def getTopic() = topic()

  def getKafkaBroker() = kafkaBroker()

  def getSessionGap() = sessionGap()

  def getWriteTopic() = writeTopic()
}
