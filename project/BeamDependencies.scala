import sbt._

object BeamDependencies {
  val beamVersion = "0.4.0"

  val beamCore = Seq(
    "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion
  )

  val beamKafka = Seq(
    "org.apache.beam" % "beam-sdks-java-io-kafka" % beamVersion
  )
}