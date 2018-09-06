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

package io.phdata.pipewrench

import java.io.File
import java.sql.JDBCType

import ai.x.diff.DiffShow
import ai.x.diff.conversions._
import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.{AppConfiguration, YamlSupport}
import io.phdata.pipeforge.jdbc.DatabaseMetadataParser
import io.phdata.pipeforge.common.jdbc.{DatabaseConf, DatabaseType}
import io.phdata.pipeforge.common.jdbc.{DataType, Column => DbColumn, Table => DbTable}
import io.phdata.pipeforge.common.{Environment => PipeforgeEnvironment}
import io.phdata.pipeforge.common.pipewrench._

import scala.util.{Failure, Success, Try}

/**
 * Pipewrench service
 */
trait Pipewrench {

  def buildAndSaveConfiguration(environment: PipeforgeEnvironment, password: String, currentFile: Option[String] = None): Unit

  /**
   * Builds a Pipewrench Configuratio from JDBC metadata
   *
   * @param databaseConf Database configuration
   * @param tableMetadata Metadata map used in Hive tblproperties
   * @param environment Pipewrench Environment
   * @return A Configuration
   */
  def buildConfiguration(databaseConf: DatabaseConf, tableMetadata: Map[String, String], environment: Environment): Try[Configuration]

  /**
   * Writes a Pipewrench Configuration to configured directory
   * @param configuration Pipewrench Configuration
   */
  def saveConfiguration(configuration: Configuration, fileName: String = "tables.yml"): Unit

  /**
   * Writes a Pipewrench Environment to configured directory
   * @param environment Pipewrench Environment
   */
  def saveEnvironment(environment: Environment, fileName: String = "environment.yml"): Unit

  /**
   * Executes Pipewrench merge command
   * @param template Template name
   * @param configuration Pipewrench Configuration
   */
  def executePipewrenchMergeApi(template: String, configuration: Configuration): Unit

  /**
   * Executes Pipewrench merge command
   * @param directory Ingest configuration directory
   * @param template Template name
   */
  def executePipewrenchMerge(directory: String, template: String): Unit

  /**
   * Verifies necessary dependencies are installed
   */
  def install(): Unit

}

class PipewrenchService() extends Pipewrench with AppConfiguration with YamlSupport with LazyLogging {

  override def buildAndSaveConfiguration(environment: PipeforgeEnvironment, password: String, currentFile: Option[String]): Unit = {
    val pipewrenchEnvironment = environment.toPipewrenchEnvironment
    buildConfiguration(environment.toDatabaseConfig(password), environment.metadata, pipewrenchEnvironment) match {
      case Success(parsedConfiguration) =>
        val mergedConfiguration: Configuration = currentFile match {
          case Some(path) =>
            val currentConfiguration = parseConfigurationFile(path)
            logger.debug(
              s"""
                 |Pipewrench configuration is different.
                 |Current configuration:
                 |$currentConfiguration
                 |Parsed configuration:
                 |$parsedConfiguration
               """.stripMargin)
            val diff = currentConfiguration.tables.diff(parsedConfiguration.tables)
            val currentTableIds = currentConfiguration.tables.diff(parsedConfiguration.tables).map(_.id)
            val mergedTables = parsedConfiguration.tables.filterNot(table => currentTableIds.contains(table.id)) ++ diff
            val conf = parsedConfiguration.copy(tables = mergedTables)
            logger.debug(DiffShow[Configuration].diff(currentConfiguration, conf).toString)
            saveConfiguration(currentConfiguration, "old_tables.yml")
            conf
          case None => parsedConfiguration
        }
        saveEnvironment(pipewrenchEnvironment)
        saveConfiguration(mergedConfiguration)
      case Failure(ex) =>
        logger.error("Failed to build Pipewrench Config", ex)
    }
  }

  /**
   * Builds a Pipewrench Configuration from JDBC metadata
   *
   * @param databaseConf Database configuration
   * @param tableMetadata Metadata map used in Hive tblproperties
   * @param environment Pipewrench Environment
   * @return A Configuration
   */
  override def buildConfiguration(databaseConf: DatabaseConf,
                                  tableMetadata: Map[String, String],
                                  environment: Environment): Try[Configuration] =
    DatabaseMetadataParser.parse(databaseConf) match {
      case Success(tables: Seq[DbTable]) =>
        logger.debug(s"Successfully parsed JDBC metadata: $tables")
        Try(
          Configuration(
            environment.name,
            environment.group,
            databaseConf.username,
            sqoop_password_file = environment.password_file,
            connection_manager = DatabaseType.getConnectionManager(databaseConf.databaseType),
            sqoop_driver = DatabaseType.getDriver(databaseConf.databaseType),
            sqoop_job_name_suffix = environment.name,
            source_database = Map("name"              -> databaseConf.schema,
                                  "cmd"               -> databaseConf.databaseType.toString,
                                  "connection_string" -> environment.connection_string),
            staging_database = Map("path"             -> environment.staging_database_path, "name" -> environment.staging_database_name),
            raw_database = Map(
              "path" -> environment.raw_database_path,
              "name" -> environment.raw_database_name
            ),
            impala_cmd = impalaCmd,
            tables = buildTables(tables, tableMetadata)
          )
        )
      case Failure(ex) =>
        logger.error("Failed to parse metadata config", ex)
        Failure(ex)
    }

  /**
   * Writes a Pipewrench Configuration to configured directory
   * @param configuration Pipewrench Configuration
   */
  override def saveConfiguration(configuration: Configuration, fileName: String = "tables.yml"): Unit = {
    val dir = projectDir(configuration.group, configuration.name)
    logger.info(s"Saving configuration: $configuration, directory: $dir")
    checkIfDirExists(dir)
    configuration.writeYamlFile(s"$dir/$fileName")
  }

  /**
   * Writes a Pipewrench Environment to configured directory
   * @param environment Pipewrench Environment
   */
  override def saveEnvironment(environment: Environment, fileName: String = "environment.yml"): Unit = {
    val dir = projectDir(environment.group, environment.name)
    logger.info(s"Saving environment: $environment, directory: $dir")
    checkIfDirExists(dir)
    environment.writeYamlFile(s"$dir/$fileName")
  }

  /**
   * Executes Pipewrench merge command
   * @param template Template name
   * @param configuration Pipewrench Configuration
   */
  override def executePipewrenchMergeApi(template: String, configuration: Configuration): Unit =
    executePipewrenchMerge(projectDir(configuration.group, configuration.name), template)

  /**
   * Executes Pipewrench merge command
   * @param directory Ingest configuration directory
   * @param template Template name
   */
  override def executePipewrenchMerge(directory: String, template: String): Unit = {
    import sys.process._
    val cmd =
      s"$pipewrenchIngestConf/generate-scripts.sh -e environment.yml -c tables.yml -p $pipewrenchDir -t $pipewrenchTemplatesDir/$template -d $directory -v $virtualInstall"
    logger.info(s"Executing cmd: $cmd")
    cmd !!
  }

  /**
   * Verifies necessary dependencies are installed
   */
  override def install(): Unit = {
    import sys.process._
    val cmd =
      s"$installScriptDir/requirements.sh -i $installScriptDir -u $pipewrenchGitUrl -c $pipewrenchIngestConf -p $pipewrenchDir -v $virtualInstall"
    logger.info(s"Checking installation requirements, executing: $cmd")
    cmd !!
  }

  /**
   * Builds Pipewrench Table object from Jdbc metadata DbTable
   * @param tables Database tables DbTable
   * @param tableMetadata A map of expanded tblproperties
   * @return A list of Tables
   */
  private def buildTables(tables: Seq[DbTable], tableMetadata: Map[String, String]): Seq[Table] =
    tables.toList
      .sortBy(_.name)
      .map { table =>
        logger.trace(s"Table definition: $table")
        val allColumns = table.primaryKeys ++ table.columns
        val pks        = table.primaryKeys.toList.sortBy(_.index).map(_.name)
        Table(
          table.name,
          Map("name" -> table.name),
          Map("name" -> cleanTableName(table.name)),
          getSplitByColumn(table),
          pks,
          Kudu(pks, 2),
          buildColumns(allColumns),
          tableMetadata,
          table.comment.replaceAll("\"", "").replaceAll("\n", " ")
        )
      }

  /**
   * Cleansing function for tables names that may contain / in their name (mainly SAP Hana tables).
   * @param name The table name
   * @return Cleansed table name
   */
  private def cleanTableName(name: String) = {
    def stripLeadingSlash(name: String) =
      if (name.startsWith("/")) {
        logger.debug(s"Table name: $name has a leading slash, this will be removed")
        name.replaceFirst("/", "")
      } else {
        name
      }
    def replaceSlash(name: String) =
      if (name.contains("/")) {
        logger.debug(s"Table name: $name contains slashes, these will be replaced with underscores")
        name.replaceAll("/", "_")
      } else {
        name
      }
    replaceSlash(stripLeadingSlash(name)).toLowerCase
  }

  /**
   * Builds Pipewrench Column objects from Jdbc metadata DbColumn
   * @param columns Database columns DbColumn
   * @return A list of Columns
   */
  private def buildColumns(columns: Set[DbColumn]): Seq[Column] =
    columns.toList
      .sortBy(_.index)
      .map { column =>
        val dataType = DataType.mapDataType(column)
        logger.trace(s"Column definition: $column, mapped dataType: $dataType")
        val columnYaml =
          Column(column.name, dataType.toString, column.comment.replaceAll("\"", "").replaceAll("\n", " "))
        if (dataType == JDBCType.DECIMAL) {
          logger.trace("Found decimal value: {}", column)
          columnYaml.copy(scale = Some(column.scale), precision = Some(column.precision))
        } else {
          columnYaml
        }
      }

  /**
   * Trys to determine which column from the table definition is the best split by column.
   * @param table Database DbTable
   * @return
   */
  def getSplitByColumn(table: DbTable) = {
    val jdbc_numerics =
      List(JDBCType.BIGINT, JDBCType.REAL, JDBCType.DECIMAL, JDBCType.DOUBLE, JDBCType.FLOAT, JDBCType.INTEGER, JDBCType.NUMERIC)
    table.primaryKeys
      .find(x => { jdbc_numerics contains x.dataType })
      .orElse(table.primaryKeys.headOption)
      .orElse(table.columns.find(x => { jdbc_numerics contains x.dataType }))
      .orElse(table.columns.headOption)
      .get
      .name
  }

  /**
   * Gets the unique directory path for a project group and name
   * @param group project group
   * @param name ingest name
   * @return path
   */
  private def projectDir(group: String, name: String) = s"$pipewrenchIngestConf/$group/$name"

  /**
   * Creates a directory in the path if it does not exist
   * @param path Directory path
   */
  private def checkIfDirExists(path: String): Unit = {
    logger.debug(s"Checking for directory: $path")
    val dir = new File(path)
    if (!dir.exists()) {
      logger.debug(s"Creating directory: $path")
      dir.mkdirs()
    }
  }
}
