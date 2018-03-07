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
import io.phdata.pipeforge.rest.domain.YamlProtocol
import io.phdata.pipeforge.rest.domain.Implicits._
import io.phdata.pipewrench.PipewrenchImpl
import org.rogach.scallop.{ ScallopConf, Subcommand }

import scala.util.{ Failure, Success }

/**
 * Pipeforge application connects to a source database and parses table definitions
 * using JDBC metadata.
 */
object Pipeforge extends YamlProtocol with LazyLogging {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val cliArgs = new CliArgsParser(args)

    cliArgs.pipewrench.databaseConf.toOption match {
      case Some(conf) =>
        val environment = parseFile(cliArgs.pipewrench.databaseConf())

        val pipewrenchConfigTry = PipewrenchImpl.buildConfig(
          environment.toDatabaseConfig(cliArgs.pipewrench.databasePassword()),
          environment.metadata,
          environment.toPipewrenchEnvironment)

        pipewrenchConfigTry match {
          case Success(pipewrenchConfig) =>
            PipewrenchImpl.writeYamlFile(pipewrenchConfig, cliArgs.pipewrench.outputPath())
          case Failure(ex) =>
            logger.error("Failed to build Pipewrench Config", ex)
        }

      case None =>
        RestApi.start(cliArgs.restApi.port())
    }

  }

  /**
   * CLI parameter parser
   *
   * Args:
   * database-configuration (s) Required - Path to the source database configuration file
   * database-password (p) Required - The source database password
   * table-metadata (m) Required - Path to the metadata yml file which will be used the enhance the tables.yml output
   * output-path (o) - Output path for the file generated tables.yml file
   * check-whitelist (c) - Output path for the file generated tables.yml file
   *
   * @param args
   */
  private class CliArgsParser(args: Seq[String]) extends ScallopConf(args) {

    val restApi = new Subcommand("rest-api") {
      lazy val port = opt[Int]("port", 'p', required = true)
    }
    addSubcommand(restApi)

    val pipewrench = new Subcommand("pipewrench") {

      lazy val databaseConf       = opt[String]("database-configuration", 's', required = true)
      lazy val databasePassword   = opt[String]("database-password", 'p', required = true)
      lazy val outputPath         = opt[String]("output-path", 'o', required = true)
      lazy val skipcheckWhitelist = opt[Boolean]("skip-whitelist-check", 'c')

    }
    addSubcommand(pipewrench)

    verify()

  }
}
