ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "TestJar",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2",
    libraryDependencies += "org.apache.hive" % "hive-exec" % "3.1.3",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.2"
  )
