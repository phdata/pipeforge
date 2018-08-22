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
lazy val appVersion = "0.13"

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
    "datanucleus" at "http://www.datanucleus.org/downloads/maven2/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    Resolver.sonatypeRepo("releases")
  )
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val dependencies =
  new {

    val akkaHttpVersion      = "10.1.1"
    val dockerTestKitVersion = "0.9.5"

    val logback           = "ch.qos.logback"             % "logback-classic"       % "1.2.3"
    val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging"        % "3.7.2"
    val typesafeConf      = "com.typesafe"               % "config"                % "1.3.0"
    val ficus             = "com.iheart"                 %% "ficus"                % "1.4.3"
    val scallop           = "org.rogach"                 %% "scallop"              % "3.1.1"
    val scalaYaml         = "net.jcazevedo"              %% "moultingyaml"         % "0.4.0"
    val mysql             = "mysql"                      % "mysql-connector-java"  % "6.0.6"
    val oracle            = "oracle"                     % "ojdbc6"                % "11.2.0.3"
    val mssql             = "com.microsoft.sqlserver"    % "mssql-jdbc"            % "6.2.2.jre8"
    val akka              = "com.typesafe.akka"          %% "akka-stream"          % "2.5.11"
    val akkaHttp          = "com.typesafe.akka"          %% "akka-http"            % akkaHttpVersion
    val akkaHttpSprayJson = "com.typesafe.akka"          %% "akka-http-spray-json" % akkaHttpVersion
    val akkaCors          = "ch.megard"                  %% "akka-http-cors"       % "0.3.0"
    val hive              = "org.apache.hive"            % "hive-jdbc"             % "1.1.0-cdh5.14.2"

    val scalaTest         = "org.scalatest"     %% "scalatest"                   % "3.0.4"              % Test
    val scalaDockerTest   = "com.whisk"         %% "docker-testkit-scalatest"    % dockerTestKitVersion % Test
    val spotifyDockerTest = "com.whisk"         %% "docker-testkit-impl-spotify" % dockerTestKitVersion % Test
    val akkaHttpTestKit   = "com.typesafe.akka" %% "akka-http-testkit"           % akkaHttpVersion      % Test
    val mockito           = "org.mockito"       % "mockito-core"                 % "2.18.3"             % Test
    val scalaMock         = "org.scalamock"     %% "scalamock-scalatest-support" % "3.6.0"              % Test

    val common = Seq(logback, scalaLogging, scalaTest, mockito, scalaMock)
  }

lazy val settings = commonSettings ++ scalafmtSettings

lazy val integrationTests = config("it") extend Test

lazy val pipeforge = project
  .in(file("."))
  .configs(integrationTests)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "pipeforge",
    version := appVersion,
    settings,
    mainClass in Compile := Some("io.phdata.pipeforge.Pipeforge"),
    libraryDependencies ++= dependencies.common ++ Seq(dependencies.scallop,
                                                       dependencies.scalaDockerTest,
                                                       dependencies.spotifyDockerTest),
    rpmLicense := Some("License: GPLv2"),
    rpmVendor := "phData"
  )
  .dependsOn(
    common,
    `jdbc-metadata`,
    pipewrench,
    `rest-api`
  )
  .aggregate(
    common,
    `jdbc-metadata`,
    pipewrench,
    `rest-api`
  )

lazy val common = project
  .settings(
    name := "common",
    version := appVersion,
    settings,
    libraryDependencies ++= dependencies.common ++ Seq(dependencies.scalaYaml,
                                                       dependencies.typesafeConf,
                                                       dependencies.ficus)
  )

lazy val `jdbc-metadata` = project
  .settings(
    name := "jdbc-metadata",
    version := appVersion,
    settings,
    libraryDependencies ++= dependencies.common ++ Seq(dependencies.mysql,
                                                       dependencies.oracle,
                                                       dependencies.mssql,
                                                       dependencies.hive)
  )
  .dependsOn(
    common
  )

lazy val pipewrench = project
  .settings(
    name := "pipewrench",
    version := appVersion,
    settings,
    libraryDependencies ++= dependencies.common
  )
  .dependsOn(
    common,
    `jdbc-metadata`
  )

lazy val `rest-api` = project
  .settings(
    name := "rest-api",
    version := appVersion,
    settings,
    libraryDependencies ++= dependencies.common ++ Seq(dependencies.akka,
                                                       dependencies.akkaHttp,
                                                       dependencies.akkaHttpSprayJson,
                                                       dependencies.akkaCors,
                                                       dependencies.akkaHttpTestKit)
  )
  .dependsOn(
    common,
    pipewrench
  )

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" * "*").get.map { f =>
    f -> s"conf/${f.name}"
  }
}

enablePlugins(JavaServerAppPackaging, UniversalDeployPlugin, RpmPlugin, RpmDeployPlugin)
