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
import sbt._

name := "pipeforge"
organization in ThisBuild := "io.phdata"
scalaVersion in ThisBuild := "2.12.3"

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "datanucleus " at "http://www.datanucleus.org/downloads/maven2/",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  }
)

lazy val dependencies =
  new {

    // Common
    val logbackVersion      = "1.2.3"
    val scalaLoggingVersion = "3.7.2"

    // JDBC
    val mysqlVersion     = "6.0.6"
    val oracleVersion    = "11.2.0.3"
    val microsoftVersion = "6.2.2.jre8"

    // CLI
    val scallopVersion   = "3.1.1"
    val scalaYamlVersion = "0.4.0"

    // Rest
    val akkaHttpVersion = "10.0.11"
    val akkaCorsVersion = "0.2.2"

    // Testing
    val scalaTestVersion     = "3.0.4"
    val dockerTestKitVersion = "0.9.5"

    // Common depends
    val logback      = "ch.qos.logback"             % "logback-classic" % logbackVersion
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging"  % scalaLoggingVersion

    // JDBC depends
    val mysql     = "mysql"                   % "mysql-connector-java" % mysqlVersion
    val oracle    = "oracle"                  % "ojdbc6"               % oracleVersion
    val microsoft = "com.microsoft.sqlserver" % "mssql-jdbc"           % microsoftVersion

    // CLI parsing depends
    val scallop   = "org.rogach"    %% "scallop"      % scallopVersion
    val scalaYaml = "net.jcazevedo" %% "moultingyaml" % scalaYamlVersion

    // Rest depends
    val akkaHttp          = "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion
    val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
    val akkaCors          = "ch.megard"         %% "akka-http-cors"       % akkaCorsVersion

    // Testing depends
    val scalaTest         = "org.scalatest" %% "scalatest"                   % scalaTestVersion     % "test"
    val scalaDockerTest   = "com.whisk"     %% "docker-testkit-scalatest"    % dockerTestKitVersion % "test"
    val spotifyDockerTest = "com.whisk"     %% "docker-testkit-impl-spotify" % dockerTestKitVersion % "test"

    val common   = Seq(logback, scalaLogging, scalaTest)
    val database = Seq(mysql, oracle, microsoft)
    val cli      = Seq(scallop, scalaYaml)
    val rest     = Seq(akkaHttp, akkaHttpSprayJson, akkaCors)
    val all      = common ++ database ++ cli ++ Seq(scalaDockerTest, spotifyDockerTest)
  }

lazy val settings = commonSettings ++ scalafmtSettings

lazy val integrationTests = config("it") extend Test

lazy val pipeforge = project
  .in(file("."))
  .configs(integrationTests)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "pipeforge",
    version := "0.1",
    settings,
    assemblySettings,
    mainClass in Compile := Some("io.phdata.pipeforge.Pipeforge"),
    libraryDependencies ++= dependencies.all
  )
  .dependsOn(
    `jdbc-metadata`,
    pipewrench,
    `rest-api`
  )
  .aggregate(
    `jdbc-metadata`,
    pipewrench,
    `rest-api`
  )

lazy val `jdbc-metadata` = project
  .settings(
    name := "jdbc-metadata",
    version := "0.1",
    settings,
    libraryDependencies ++= dependencies.common ++ dependencies.database
  )

lazy val pipewrench = project
  .settings(
    name := "pipewrench",
    version := "0.1",
    settings,
    libraryDependencies ++= dependencies.common ++ Seq(
      dependencies.scalaYaml
    )
  )
  .dependsOn(
    `jdbc-metadata`
  )

lazy val `rest-api` = project
  .settings(
    name := "rest-api",
    version := "0.1-SNAPSHOT",
    settings,
    libraryDependencies ++= dependencies.common ++ dependencies.rest
  )
  .dependsOn(
    pipewrench
  )

enablePlugins(JavaAppPackaging)
