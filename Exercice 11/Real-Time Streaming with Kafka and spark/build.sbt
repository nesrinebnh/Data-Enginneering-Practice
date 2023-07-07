ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "Real-Time Streaming with Kafka and spark",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.5.4",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.5",
      "io.circe" %% "circe-generic" % "0.14.5",
      "io.circe" %% "circe-parser" % "0.14.5"
    )

  )
