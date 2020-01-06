name := "hate-speech-prediction"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",

  //config factory for configuring spark
  "com.typesafe" % "config" % "1.4.0",
  // https://mvnrepository.com/artifact/log4j/log4j
  "log4j" % "log4j" % "1.2.17",

  //stanford nlp library
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp")),
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1"
)