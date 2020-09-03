name := "SparkKafka_DE"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.3"

val sparkVersion = "2.3.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  // streaming-kafka
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.3"
)

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.3.3" excludeAll(excludeJpountz)