package com.getindata.serialization

import java.nio.charset.StandardCharsets

import com.getindata.Event
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import spray.json._

class JsonEventSerializationSchema extends SerializationSchema[Event] with DeserializationSchema[Event] {
  override def serialize(element: Event): Array[Byte] = element.toJson.toString().getBytes(StandardCharsets.UTF_8)

  override def isEndOfStream(nextElement: Event): Boolean = false

  override def deserialize(message: Array[Byte]): Event = new String(message,
    StandardCharsets.UTF_8).parseJson.convertTo[Event]

  override def getProducedType: TypeInformation[Event] = TypeInformation.of(classOf[Event])
}
