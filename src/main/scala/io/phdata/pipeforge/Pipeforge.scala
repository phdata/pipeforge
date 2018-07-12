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
import io.phdata.pipeforge.common.YamlSupport
import io.phdata.pipeforge.jdbc.SchemaValidator
import io.phdata.pipewrench.PipewrenchService
import org.rogach.scallop.{ ScallopConf, Subcommand }

import scala.io.StdIn

/**
 * The purpose of the Pipeforge application is build Pipewrench configuration files by parsing metadata
 * from a source database.
 */
object Pipeforge extends YamlSupport with LazyLogging {

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
        val environment = parseEnvironmentFile(cliArgs.configuration.environment())
        logger.info(s"Building Pipewrench configuration from environment: $environment")

        // If password is not supplied via CLI parameter then ask the user for it
        val password =
          getArgOrAsk(cliArgs.configuration.databasePassword.toOption, "Enter database password: ")

        pipewrenchService.buildAndSaveConfiguration(environment, password)

      case Some(cliArgs.merge) =>
        logger.info("Running Pipewrench merge from command line")
        pipewrenchService.install()
        pipewrenchService.executePipewrenchMerge(cliArgs.merge.directory(),
                                                 cliArgs.merge.template())
      case Some(cliArgs.validateSchema) =>
        logger.info("Executing schema validation")
        val environment = parseEnvironmentFile(cliArgs.validateSchema.environment())
        val databasePassword =
          getArgOrAsk(cliArgs.validateSchema.databasePassword.toOption, "Enter database password: ")
        val hadoopPassword =
          getArgOrAsk(cliArgs.validateSchema.hadoopPassword.toOption, "Emter Hadoop password: ")

        SchemaValidator.validateSchema(environment, databasePassword, hadoopPassword)

      case _ => // parsing failure
    }
  }

  private def getArgOrAsk(cliArg: Option[String], msg: String): String =
    cliArg match {
      case Some(password) => password
      case None =>
        print(msg)
        StdIn.readLine()
    }

  /**
   * CLI parameter parser*
   *
   * Subcommands:
   *   - pipewrench: Connects to source database and writes parsed Pipewrench Configuration
   *       Args:
   *       environment (e) Required - Path to the source database configuration file
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
        opt[String]("password", 'p', descr = "database password", required = false)

    }
    addSubcommand(configuration)

    val merge = new Subcommand("merge") {
      descr("Build pipewrench table.yml and environment.yml")
      val directory =
        opt[String]("directory", 'd', descr = "Pipewrench configuration directory", required = true)
      val template = opt[String]("template", 't', descr = "Pipewrench template", required = true)
    }
    addSubcommand(merge)

    val validateSchema = new Subcommand("validate-schema") {
      descr("Validate source database schema against Hive Metastore")
      val environment =
        opt[String]("environment", 'e', descr = "environment.yml file", required = true)
      val databasePassword =
        opt[String]("database-password", 'p', descr = "database password", required = false)
      val hadoopPassword =
        opt[String]("hadoop-password", 'h', descr = "impala password", required = false)
    }
    addSubcommand(validateSchema)

    verify()

  }

}
