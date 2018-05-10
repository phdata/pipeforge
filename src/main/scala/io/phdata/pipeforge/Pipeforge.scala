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
import io.phdata.pipeforge.rest.RestApp
import io.phdata.pipeforge.rest.domain.YamlSupport
import io.phdata.pipewrench.PipewrenchService
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

    val pipewrenchService = new PipewrenchService()

    cliArgs.subcommand match {
      case Some(cliArgs.restApi) =>
        logger.info("Starting Pipeforge rest api")
        new RestApp(pipewrenchService).start(cliArgs.restApi.port())
      case Some(cliArgs.configuration) =>
        // Parse file into Environment
        val environment = parseFile(cliArgs.configuration.environment())
        logger.info(s"Building Pipewrench configuration from environment: $environment")
        // Build Pipewrench Environment from Pipeforge environment
        val pipewrenchEnv = environment.toPipewrenchEnvironment

        val skipWhiteListCheck = cliArgs.configuration.skipcheckWhitelist.getOrElse(false)
        // Build Pipewrench Configuration
        pipewrenchService.buildConfiguration(
          environment.toDatabaseConfig(cliArgs.configuration.databasePassword()),
          environment.metadata,
          pipewrenchEnv,
          skipWhiteListCheck) match {
          case Success(configuration) =>
            pipewrenchService.saveEnvironment(pipewrenchEnv)
            pipewrenchService.saveConfiguration(configuration)
          case Failure(ex) =>
            logger.error("Failed to build Pipewrench Config", ex)
        }
      case Some(cliArgs.merge) =>
        logger.info("Running Pipewrench merge from command line")
        pipewrenchService.install()
        pipewrenchService.executePipewrenchMerge(cliArgs.merge.directory(),
                                                 cliArgs.merge.template())
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

    val configuration = new Subcommand("configuration") {
      descr("Build pipewrench table.yml and environment.yml")

      val environment =
        opt[String]("environment", 'e', descr = "environment.yml file", required = true)
      val databasePassword =
        opt[String]("password", 'p', descr = "database password", required = true)
      val skipcheckWhitelist = opt[Boolean](
        "override-whitelist-check",
        'c',
        descr = "Skips checking whitelisted tables against source database",
        default = Some(false))

    }
    addSubcommand(configuration)

    val merge = new Subcommand("merge") {
      descr("Build pipewrench table.yml and environment.yml")
      val directory =
        opt[String]("directory", 'd', descr = "Pipewrench configuration directory", required = true)
      val template = opt[String]("template", 't', descr = "Pipewrench template", required = true)
    }
    addSubcommand(merge)

    verify()

  }

}
