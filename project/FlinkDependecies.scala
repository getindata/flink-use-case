import sbt._

object FlinkDependecies {

  val flinkVersion = "1.1.4"

  val flinkCore = Seq(
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
  )

  val flinkKafka = Seq(
    "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion % "provided"
  )
}