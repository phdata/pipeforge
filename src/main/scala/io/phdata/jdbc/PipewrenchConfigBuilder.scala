package io.phdata.jdbc

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.DatabaseConf
import io.phdata.jdbc.domain.Table
import io.phdata.jdbc.parsing.DatabaseMetadataParser
import io.phdata.jdbc.pipewrench.TableBuilder
import io.phdata.jdbc.util.YamlWrapper
import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success}

/**
  * Pipewrench config builder application connects to a source database and parses table definitions
  * using JDBC metadata.
  */
object PipewrenchConfigBuilder extends LazyLogging {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val cliArgs = new CliArgsParser(args)

    // Read and parse database-configuration file
    val sourceDbConf = DatabaseConf.parse(cliArgs.databaseConf(), cliArgs.databasePassword())
    // Try to parse database metadata
    DatabaseMetadataParser.parse(sourceDbConf, cliArgs.skipcheckWhitelist.getOrElse(false)) match {
      case Success(databaseMetadata) =>
        // Read in additional table metadata
        val tableMetadata = YamlWrapper.read(cliArgs.tableMetadata())
        // Build Pipewrench tables definition
        val generatedConfig = buildPipewrenchConfig(databaseMetadata, tableMetadata)
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
    lazy val databaseConf = opt[String]("database-configuration", 's', required = true)
    lazy val databasePassword = opt[String]("database-password", 'p', required = false)
    lazy val tableMetadata = opt[String]("table-metadata", 'm', required = false)
    lazy val outputPath = opt[String]("output-path", 'o', required = true)
    lazy val skipcheckWhitelist = opt[Boolean]("skip-whitelist-check", 'c')

    verify()
  }

  def buildPipewrenchConfig(databaseMetadata: Set[Table], tableMetadata: Map[String, Object]) =
    Map("tables" -> TableBuilder.buildTablesSection(databaseMetadata, tableMetadata))
}
