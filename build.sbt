/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt.Keys.{test, _}
import sbt._

lazy val IntegrationTest = config("it") extend (Test)

parallelExecution in Test := false

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(name := "Pipeforge",
    version := "0.1-SNAPSHOT",
    organization := "io.phdata",
    scalaVersion := "2.12.3",
    mainClass in Compile := Some("io.phdata.jdbc.PipewrenchConfigBuilder"),
    resolvers += "datanucleus " at "http://www.datanucleus.org/downloads/maven2/",
    libraryDependencies ++= Seq(
      "mysql" % "mysql-connector-java" % "6.0.6",
      "oracle" % "ojdbc6" % "11.2.0.3",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.2.jre8",
      "org.yaml" % "snakeyaml" % "1.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.typesafe" % "config" % "1.3.1",
      // https://github.com/iheartradio/ficus
      "com.iheart" %% "ficus" % "1.4.3",
      // https://github.com/scallop/scallop
      "org.rogach" %% "scallop" % "3.1.1",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "com.whisk" %% "docker-testkit-scalatest" % "0.9.5" % "test",
      "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.5" % "test"
    ),
    test in assembly := {}
  )

enablePlugins(JavaAppPackaging)