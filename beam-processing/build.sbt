name := "beam-processing"

libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "2.0.6"
)


assemblyMergeStrategy in assembly := {
  case PathList("com", "google", "protobuf", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}