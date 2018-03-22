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

package io.phdata.pipeforge

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.RestApi
import io.phdata.pipeforge.rest.domain.YamlSupport
import io.phdata.pipeforge.rest.domain.Implicits._
import io.phdata.pipewrench.PipewrenchImpl
import io.phdata.pipewrench.domain.{ YamlSupport => PipewrenchYamlSupport }
import org.rogach.scallop.{ ScallopConf, Subcommand }

import scala.util.{ Failure, Success }

/**
 * The purpose of the Pipeforge application is build Pipewrench configuration files by parsing metadata
 * from a source database.
 */
object Pipeforge extends YamlSupport with PipewrenchYamlSupport with LazyLogging {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val cliArgs = new CliArgsParser(args)

    cliArgs.subcommand match {
      case Some(cliArgs.restApi) =>
        logger.info("Starting Pipeforge rest api")
        RestApi.start(cliArgs.restApi.port())
      case Some(cliArgs.pipewrench) =>
        logger.info("pipewrench called")
        // Parse file into Environment
        val environment = parseFile(cliArgs.pipewrench.databaseConf())
        // Build Pipewrench Environment from Pipeforge environment
        val pipewrenchEnv = environment.toPipewrenchEnvironment

        logger.debug(s"Parsed environment: $environment")
        logger.debug(s"Pipewrench environment: $pipewrenchEnv")

        // Build Pipewrench Configuration
        PipewrenchImpl.buildConfiguration(
          environment.toDatabaseConfig(cliArgs.pipewrench.databasePassword()),
          environment.metadata,
          pipewrenchEnv) match {
          case Success(configuration) =>
            val path = cliArgs.pipewrench.outputPath()
            logger.debug(s"Pipewrench Configuraiton: $configuration")
            configuration.writeYamlFile(s"$path/tables.yml")
            pipewrenchEnv.writeYamlFile(s"$path/env.yml")
          case Failure(ex) =>
            logger.error("Failed to build Pipewrench Config", ex)
        }
      case _ => // parsing failure
    }
  }

  /**
   * CLI parameter parser*
   *
   * Subcommands:
   *   - pipewrench: Connects to source database and writes parsed Pipewrench Configuration
   *       Args:
   *       enviornment (e) Required - Path to the source database configuration file
   *       password (p) Required - The source database password
   *       output-path (o) - Output path for the file generated tables.yml file
   *       check-whitelist (c) - Output path for the file generated tables.yml file
   *
   *  - rest-api: Exposes a rest
   *      Args:
   *        port (p) Required - Port to expose rest service on
   *
   * @param args
   */
  class CliArgsParser(args: Seq[String]) extends ScallopConf(args) {

    val restApi = new Subcommand("rest-api") {
      descr("Start Pipeforge rest-api")
      val port = opt[Int]("port", 'p', descr = "Port", required = true)
    }
    addSubcommand(restApi)

    val pipewrench = new Subcommand("pipewrench") {
      descr("Build pipewrench table.yml and environment.yml")
      val databaseConf =
        opt[String]("environment", 'e', descr = "environment.yml file", required = true)
      val databasePassword =
        opt[String]("password", 'p', descr = "database password", required = true)
      val outputPath = opt[String]("output-path", 'o', descr = "output path", required = true)
      val skipcheckWhitelist = opt[Boolean](
        "override-whitelist-check",
        'c',
        descr = "Skips checking whitelisted tables against source database",
        default = Some(true))

    }
    addSubcommand(pipewrench)

    verify()

  }

}
