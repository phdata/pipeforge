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
  * Query a source database via JDBC and output generated Pipewrench config
  * (`tables.yml`) for all tables in a given schema.
  */
object PipewrenchConfigBuilder extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val cliArgs = new CliArgsParser(args)

    val sourceDbConf = DatabaseConf.parse(cliArgs.databaseConf(), cliArgs.databasePassword())
    DatabaseMetadataParser.parse(sourceDbConf) match {
      case Success(databaseMetadata) =>
        val tableMetadata = YamlWrapper.read(cliArgs.tableMetadata())
        val generatedConfig = buildPipewrenchConfig(databaseMetadata, tableMetadata)
        YamlWrapper.write(generatedConfig, cliArgs.outputPath)
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
    *
    * @param args
    */
  private class CliArgsParser(args: Seq[String]) extends ScallopConf(args) {
    lazy val databaseConf = opt[String]("database-configuration", 's', required = true)
    lazy val databasePassword = opt[String]("database-password", 'p', required = true)
    lazy val tableMetadata = opt[String]("table-metadata", 'm', required = true)
    lazy val outputPath = opt[String]("output-path", 'o', required = false).getOrElse("tables.yml")

    verify()
  }

  def buildPipewrenchConfig(databaseMetadata: Set[Table], tableMetadata: Map[String, Object]) =
    Map("tables" -> TableBuilder.buildTablesSection(databaseMetadata, tableMetadata))
}
