package com.getindata

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.SerializationSchema
import org.rogach.scallop.ScallopConf
import com.getindata.serialization._

object GenerateEventsJob {

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val maxInterval = opt[Long](required = false, descr = "Maximal interval in millis between two consecutive events",
      default = Some(1000L))
    val topic = opt[String](required = false, descr = "Kafka topic to write", default = Some("songs"))
    val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")
    val maxTimeDeviation = opt[Long](required = false,
      descr = "Maximal deviation from current time while assigning event timestamp",
      default = Some(0L))

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
    val stream = env.addSource(new GeneratedEventsSource(conf.maxInterval(), conf.maxTimeDeviation()))
      .name("Event generator")

    val serializationSchema = getSerializationSchema
    val kafkaProducer = new FlinkKafkaProducer09[Event](conf.topic(),
      serializationSchema,
      kafkaProperties(conf.kafkaBroker()))

    stream.addSink(kafkaProducer).name("Write to kafka")

    env.execute("Generate events")
  }


  private def getSerializationSchema = {
    new JsonEventSerializationSchema
  }
}
