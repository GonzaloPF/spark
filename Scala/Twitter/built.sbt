scalaVersion := "2.11.8"

name := "word"
organization := "dataproc.codelab"
version := "1.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

