import sbt._

object Dependencies {


  lazy val kafkaClientsDeps: List[ModuleID] =
    "org.apache.kafka" % "kafka-clients" % "3.4.0" ::
      "io.confluent" % "kafka-avro-serializer" % "6.0.0" :: Nil
}