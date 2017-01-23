package com.getindata

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.rogach.scallop.ScallopConf

object GenerateEventsJob {

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val maxInterval = opt[Int](required = false, descr = "Maximal interval in millis between two consecutive events",
      default = Some(1000))
    val topic = opt[String](required = false, descr = "Kafka topic to write", default = Some("songs"))
    val kafkaBroker = trailArg[String](required = true, descr = "Kafka broker list")

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
    val stream = env.addSource(new GeneratedEventsSource(conf.maxInterval())).name("Event generator")

    val serializationSchema = getSerializationSchema(env)
    val kafkaProducer = new FlinkKafkaProducer09[Event](conf.topic(),
      serializationSchema,
      kafkaProperties(conf.kafkaBroker()))

    stream.addSink(kafkaProducer)

    env.execute("Generate events")
  }

  private def getSerializationSchema(env: StreamExecutionEnvironment) = {
    new TypeInformationSerializationSchema[Event](TypeInformation.of(classOf[Event]), env.getConfig)
  }
}
