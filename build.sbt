import FlinkDependecies._

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  organization := "com.getindata"
)

lazy val eventGenerator = (project in file("event-generator"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= (flinkCore ++ flinkKafka))
  .dependsOn(modelClasses)

lazy val modelClasses = (project in file("model-classes"))
  .settings(commonSettings)

lazy val root = (project in file(".")).aggregate(eventGenerator)