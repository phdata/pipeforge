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
import io.phdata.pipewrench.TableBuilder
import io.phdata.pipewrench.util.YamlWrapper
import org.rogach.scallop.ScallopConf

import scala.util.{ Failure, Success }

/**
 * Pipewrench config builder application connects to a source database and parses table definitions
 * using JDBC metadata.
 */
object PipewrenchConfigBuilder extends LazyLogging {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val cliArgs = new CliArgsParser(args)

    val databaseConf = EnvironmentYaml.getDatabaseConf(cliArgs.databaseConf(), cliArgs.databasePassword())

    // Try to parse database metadata
    DatabaseMetadataParser.parse(databaseConf, cliArgs.skipcheckWhitelist.getOrElse(false)) match {
      case Success(databaseMetadata) =>
        // Read in additional table metadata
        val tableMetadata = YamlWrapper.read(cliArgs.tableMetadata())
        // Build Pipewrench tables definition
        val generatedConfig = Map(
          "tables" -> TableBuilder.buildTablesSection(databaseMetadata, tableMetadata))
        // Write Pipewrench tables files
        YamlWrapper.write(generatedConfig, cliArgs.outputPath())
      case Failure(e) =>
        logger.error("Error gathering metadata from source", e)
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
    lazy val databaseConf       = opt[String]("database-configuration", 's', required = true)
    lazy val databasePassword   = opt[String]("database-password", 'p', required = true)
    lazy val tableMetadata      = opt[String]("table-metadata", 'm', required = true)
    lazy val outputPath         = opt[String]("output-path", 'o', required = true)
    lazy val skipcheckWhitelist = opt[Boolean]("skip-whitelist-check", 'c')

    verify()
  }
}