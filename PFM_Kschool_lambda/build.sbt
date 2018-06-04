name := "PFM_Kschool_lambda"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models-spanish"
)