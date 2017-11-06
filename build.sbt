name := "jdbc-metadata-parser"
version := "0.1-SNAPSHOT"

organization := "io.phdata"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.3"

// resolver for ojdb6.jar
resolvers += "datanucleus " at "http://www.datanucleus.org/downloads/maven2/"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "6.0.6",
  "oracle" % "ojdbc6" % "11.2.0.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)
