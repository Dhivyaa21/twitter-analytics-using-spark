name := "sentiment-analysis"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += Resolver.sonatypeRepo("releases")

publishTo := Some(Resolver.file("file", new File("target/")))

libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0-preview2" % "runtime",

//config factory for configuring spark
  "com.typesafe" % "config" % "1.4.0",
  // https://mvnrepository.com/artifact/log4j/log4j
  "log4j" % "log4j" % "1.2.17",

  //stanford nlp library
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" artifacts(Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp")),
  //"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "org.apache.kafka" %% "kafka" % "2.4.0"
)

assemblyMergeStrategy in assembly := {
  case "module-info.class" => MergeStrategy.discard
  case PathList("META-INF", "versions", "9", "javax", "xml", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "xml", "bind", xs@_*) => MergeStrategy.last
  case PathList("com", "sun", "xml", "bind", xs@_*) => MergeStrategy.last
  case PathList("com", "sun", "istack", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "hadoop", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "commons", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case PathList(ps@_*) if ps.last endsWith "git.properties" => MergeStrategy.last
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

addArtifact(artifact in(Compile, assembly), assembly)