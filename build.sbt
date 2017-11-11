import sbt.Keys.{test, _}
import sbt._

lazy val IntegrationTest = config("it") extend (Test)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(name := "Pipeforge",
    version := "0.1-SNAPSHOT",
    organization := "io.phdata",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.12.3",
    mainClass in Compile := Some("io.phdata.jdbc.PipewrenchConfigBuilder"),
    resolvers += "datanucleus " at "http://www.datanucleus.org/downloads/maven2/",
    libraryDependencies ++= Seq(
      "mysql" % "mysql-connector-java" % "6.0.6",
      "oracle" % "ojdbc6" % "11.2.0.3",
      "org.yaml" % "snakeyaml" % "1.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.typesafe" % "config" % "1.3.1",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "org.testcontainers" % "oracle-xe" % "1.4.3" % "test",
      "org.testcontainers" % "mysql" % "1.4.3" % "test"
    ),
    test in assembly := {}
  )
