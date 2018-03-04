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
import io.phdata.pipeforge.config.EnvironmentYaml
import io.phdata.pipeforge.jdbc.DatabaseMetadataParser
import io.phdata.pipeforge.rest.RestApi
import io.phdata.pipewrench.Pipewrench
import io.phdata.pipewrench.domain.TableMetadataYamlProtocol
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.util.{Failure, Success}

/**
 * Pipewrench config builder application connects to a source database and parses table definitions
 * using JDBC metadata.
 */
object PipewrenchConfigBuilder extends LazyLogging {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val cliArgs = new CliArgsParser(args)

    cliArgs.pipewrench.databaseConf.toOption match {
      case Some(conf) =>
        logger.info("pipewrench")
        pipewrenchCmd(conf, cliArgs.pipewrench.databasePassword(), cliArgs.pipewrench.outputPath(), cliArgs.pipewrench.tablesMetadata(), cliArgs.pipewrench.skipcheckWhitelist.toOption)
      case None =>
        logger.info("rest-api")
        RestApi.start(cliArgs.restApi.port())
    }

  }

  def pipewrenchCmd(conf: String, password: String, outputPath: String, tablesMetadataPath: String, skipWhiteList: Option[Boolean] = None): Unit = {
    val databaseConf =
      EnvironmentYaml.getDatabaseConf(conf, password)

    // Parse additional table metadata config
    val metadata = TableMetadataYamlProtocol.parseTablesMetadata(tablesMetadataPath)

    // Try to parse database metadata
    DatabaseMetadataParser.parse(databaseConf, skipWhiteList.getOrElse(false)) match {
      case Success(tables) =>
        Pipewrench.buildConfig(tables, outputPath, metadata)
      case Failure(e) => logger.error("Error gathering metadata from source", e)
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
      lazy val tablesMetadata     = opt[String]("tables-metadata", 'm', required = true)
      lazy val skipcheckWhitelist = opt[Boolean]("skip-whitelist-check", 'c')

      //pipewrenchCmd(databaseConf(), databasePassword(), outputPath(), tablesMetadata(), skipcheckWhitelist.toOption)


    }
    addSubcommand(pipewrench)

    verify()

  }
}
