name := "Jdbc-Metadata-Parser"
version := "0.1-SNAPSHOT"

organization := "io.phdata"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "6.0.6",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
)