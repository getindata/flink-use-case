import FlinkDependecies._

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  organization := "com.getindata",
  run in Compile <<=
    Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
)

lazy val modelClasses = (project in file("model-classes"))
  .settings(commonSettings)

lazy val eventGenerator = (project in file("event-generator"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= (flinkCore ++ flinkKafka))
  .dependsOn(modelClasses)

lazy val flinkProcessing = (project in file("flink-processing"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= (flinkCore ++ flinkKafka))
  .dependsOn(modelClasses)

lazy val root = (project in file(".")).aggregate(eventGenerator, flinkProcessing)

