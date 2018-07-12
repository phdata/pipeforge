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

package io.phdata.pipeforge.jdbc

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.jdbc._
import io.phdata.pipeforge.jdbc.Implicits._

import scala.util.{ Failure, Success, Try }

/**
 * Parses table definitions from a database
 */
trait DatabaseMetadataParser extends LazyLogging {

  /**
   * Database connection
   * @return Connection
   */
  def connection: Connection

  /**
   * Database specific query that returns a result set containing all tables in the specified schema
   * @param schema Schema or database name
   * @return SQL query listing databases in schema
   */
  def listTablesStatement(schema: String): String

  /**
   * Database specific query selecting a single row from the a schema and table
   * @param schema Schema or database name
   * @param table Table name
   * @return SQL query selecting a single row from table
   */
  def singleRecordQuery(schema: String, table: String): String

  /**
   * Secondary SQL query for use when tables do not have any records.  This query will left outer join to another
   * table to return a record with all NULL records.
   * @param schema Schema or database name
   * @param table Table name
   * @return SQL query selecting a single row from a table
   */
  def joinedSingleRecordQuery(schema: String, table: String): String

  /**
   * Database specific query that returns a result set containing all views in the specified schema
   * @param schema Schema or database name
   * @return
   */
  def listViewsStatement(schema: String): String

  /**
   * Database specific query that returns the table comment
   * @param schema Database schema
   * @param table Database table
   * @return The table comment query
   */
  def tableCommentQuery(schema: String, table: String): Option[String]

  /**
   * Database specific query that returns the column comments for the specified schema and table
   * @param schema Database schema
   * @param table Database table
   * @return The column comment query
   */
  def columnCommentsQuery(schema: String, table: String): Option[String]

  /**
   * Build column definitions for a specific table
   * @param schema Schema or database name
   * @param table Table name
   * @return A Set of Column definitions
   */
  def getColumnDefinitions(schema: String, table: String): Try[Set[Column]] = {
    val query = singleRecordQuery(schema, table)
    logger.debug(s"Gathering column definitions for $schema.$table, query: {}", query)
    val stmt = connection.createStatement()
    val metaDataResult: Try[ResultSetMetaData] =
      stmt.executeQuery(query).toStream.map(_.getMetaData).toList.headOption match {
        case Some(metaData) => Success(metaData)
        case None =>
          val joinedQuery = joinedSingleRecordQuery(schema, table)
          logger.debug(s"Table: $table does not contain any records, using joined query: {}",
                       joinedQuery)
          try {
            stmt.executeQuery(joinedQuery).toStream.map(_.getMetaData).toList.headOption match {
              case Some(metaData) => Success(metaData)
              case None =>
                Failure(new Exception(s"Failed to use join query to get column definitions"))
            }
          } catch {
            case e: Exception => Failure(e)
          }
      }

    val result: Try[Set[Column]] = metaDataResult match {
      case Success(metaData) =>
        val rsMetadata     = metaData.asInstanceOf[java.sql.ResultSetMetaData]
        val columnComments = getColumnComments(schema, table)
        Success(mapMetaDataToColumn(columnComments, metaData, rsMetadata))
      case Failure(ex) => Failure(ex)
    }

    stmt.close()
    result
  }

  /**
   * Main starting point for gathering table and column metadata
   * @param objectType Table or View
   * @param schema Schema or database name
   * @param tableWhiteList Optional table whitelisting
   * @return A Set of table definitions
   */
  def getTablesMetadata(objectType: ObjectType.Value,
                        schema: String,
                        tableWhiteList: Option[List[String]] = None,
                        skipWhiteListCheck: Boolean = false): Try[List[Table]] =
    // Query database for a list of tables or views
    tableWhiteList match {
      case Some(tables) =>
        if (skipWhiteListCheck) {
          Try(tables.flatMap(getTableMetadata(schema, _)))
        } else {
          logger.debug("Checking user supplied white list against source system: {}", tables)
          val sourceTables = listTables(objectType, schema)
          if (tables.toSet.subsetOf(sourceTables.toSet)) {
            Try(tables.flatMap(getTableMetadata(schema, _)))
          } else {
            Failure(new Exception(
              s"A table in the whitelist was not found in the source system, whitelist=$tables, source tables=$sourceTables"))
          }
        }
      case None =>
        val sourceTables = listTables(objectType, schema)
        Try(sourceTables.flatMap(getTableMetadata(schema, _)))
    }

  /**
   * Gets metadata for an individual table and its columns
   * @param schema Schema or database name
   * @param table Table name
   * @return Table definition
   */
  def getTableMetadata(schema: String, table: String): Option[Table] =
    getColumnDefinitions(schema, table) match {
      case Success(allColumns) =>
        val pks     = primaryKeys(schema, table, allColumns)
        val columns = allColumns.diff(pks)
        Some(
          Table(table, getTableComment(schema, table).getOrElse(""), pks, columns)
        )
      case Failure(ex) =>
        logger.warn(s"Failed to get metadata for table:$table", ex)
        None
    }

  /**
   * Gets table comments from the source system, user will need access to the sys or information_schema schemas
   * to read table comments.  A blank comment will be used if access has not been granted.
   *
   * @param schema Database schema
   * @param table Table schema
   * @return The table comment
   */
  def getTableComment(schema: String, table: String): Option[String] = {
    val stmt = connection.createStatement()
    try {
      tableCommentQuery(schema, table) match {
        case Some(query) =>
          logger.debug("Getting table comments, query: {}", query)
          stmt.executeQuery(query).toStream.map(rs => rs.getString(1)).headOption
        case None => Some("")
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to query source for table comment, defaulting to empty comment", e)
        // If the query fails here it is most likely due to the user not having permissions
        // Instead of failing we need to capture the exception and return an empty comment
        Some("")
    } finally {
      stmt.close()
    }
  }

  /**
   * Gets column comments from the source system, user will need access to the sys or information_schema schemas
   * to read column comments.  A blank comment will be used if access has not been granted.
   *
   * @param schema Database schema
   * @param table Database table
   * @return A list containing (column, comment)
   */
  def getColumnComments(schema: String, table: String): List[(String, Option[String])] = {
    val stmt = connection.createStatement()
    try {
      columnCommentsQuery(schema, table) match {
        case Some(query) =>
          logger.debug("Getting column comments, query: {}", query)
          stmt
            .executeQuery(query)
            .toStream
            .map(rs => (rs.getString(1), Option(rs.getString(2))))
            .toList
        case None =>
          List[(String, Option[String])]()
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to query source for column comments, defaulting to empty comments", e)
        // If the query fails here it is most likely due to the user not having permissions
        // Instead of failing we need to capture the exception and return an empty list of comments
        List[(String, Option[String])]()
    } finally {
      stmt.close()
    }
  }

  /**
   * Gets the primary keys for a table
   * @param schema Schema or database name
   * @param table Table name
   * @param columns Complete set of column definitions for the table
   * @return Primary key definitions
   */
  def primaryKeys(schema: String, table: String, columns: Set[Column]): Set[Column] = {
    logger.debug("Gathering primary keys from JDBC metadata")
    try {
      val pks = connection.getMetaData
        .getPrimaryKeys(schema, schema, table)
        .toStream
        .map(record => record.getString("COLUMN_NAME") -> record.getInt("KEY_SEQ"))
        .toMap

      mapPrimaryKeyToColumn(pks, columns)
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to get primary keys for schema: $schema, table: $table from source system",
          e)
        Set[Column]()
    }
  }

  /**
   * Maps a JDBC result set to Column definition
   * @param metaData Result set metadata
   * @param rsMetadata
   * @return A set of column definitions
   */
  def mapMetaDataToColumn(columnComments: List[(String, Option[String])],
                          metaData: ResultSetMetaData,
                          rsMetadata: ResultSetMetaData): Set[Column] = {
    def asBoolean(i: Int) = if (i == 0) false else true

    (1 to metaData.getColumnCount).map { i =>
      val columnName = metaData.getColumnName(i)
      val comment = columnComments.find(f => columnName == f._1) match {
        case Some((column, commentOpt)) => commentOpt.getOrElse("")
        case None                       => ""
      }

      Column(
        columnName,
        comment,
        JDBCType.valueOf(rsMetadata.getColumnType(i)),
        asBoolean(metaData.isNullable(i)),
        i,
        metaData.getPrecision(i),
        metaData.getScale(i)
      )
    }.toSet
  }

  /**
   * Finds the column definitions for primary key definitions
   * @param primaryKeys A Map containing the primary key and its column position
   * @param columns A Set of column definitions
   * @return Primary key column definitions
   */
  def mapPrimaryKeyToColumn(primaryKeys: Map[String, Int], columns: Set[Column]): Set[Column] =
    primaryKeys.flatMap {
      case (key, index) =>
        columns.find(_.name == key) match {
          case Some(column) => Some(column)
          case None         => None
        }
    }.toSet

  /**
   * Gathers a list of tables or views from the specified schema in the database
   * @param objectType Table or View
   * @param schema Schema or database name
   * @return A Set of tables or views
   */
  def listTables(objectType: ObjectType.Value, schema: String): List[String] = {
    val stmt: Statement = connection.createStatement()
    val query =
      if (objectType == ObjectType.TABLE) listTablesStatement(schema)
      else listViewsStatement(schema)
    logger.debug(s"Getting list of source ${objectType.toString}s, query: {}", query)
    val result = stmt.executeQuery(query).toStream.map(_.getString(1)).toList
    stmt.close()
    result
  }

}

/**
 * Parses table definitions from database
 */
object DatabaseMetadataParser extends LazyLogging {

  /**
   * Parses table definition from database
   * @param configuration Database configuration
   * @return Set of table definitions
   */
  def parse(configuration: DatabaseConf, skipWhiteListCheck: Boolean = false): Try[List[Table]] = {
    logger.info("Extracting metadata information from database: {}",
                configuration.copy(password = "******"))

    // Establish connection to database
    getConnection(configuration) match {
      case Success(connection) =>
        // Determine the database type and parse table definitions
        configuration.databaseType match {
          case DatabaseType.MYSQL =>
            new MySQLMetadataParser(connection)
              .getTablesMetadata(configuration.objectType,
                                 configuration.schema,
                                 configuration.tables,
                                 skipWhiteListCheck)
          case DatabaseType.ORACLE =>
            new OracleMetadataParser(connection)
              .getTablesMetadata(configuration.objectType,
                                 configuration.schema,
                                 configuration.tables,
                                 skipWhiteListCheck)
          case DatabaseType.MSSQL =>
            new MsSQLMetadataParser(connection)
              .getTablesMetadata(configuration.objectType,
                                 configuration.schema,
                                 configuration.tables,
                                 skipWhiteListCheck)
          case DatabaseType.HANA =>
            new HANAMetadataParser(connection)
              .getTablesMetadata(configuration.objectType,
                                 configuration.schema,
                                 configuration.tables,
                                 skipWhiteListCheck)
          case DatabaseType.TERADATA =>
            new TeradataMetadataParser(connection)
              .getTablesMetadata(configuration.objectType,
                                 configuration.schema,
                                 configuration.tables,
                                 skipWhiteListCheck)
          case DatabaseType.AS400 =>
            new AS400MetadataParser(connection).getTablesMetadata(configuration.objectType,
                                                                  configuration.schema,
                                                                  configuration.tables,
                                                                  skipWhiteListCheck)
          case DatabaseType.REDSHIFT =>
            new RedshiftMetadataParser(connection).getTablesMetadata(configuration.objectType,
                                                                     configuration.schema,
                                                                     configuration.tables,
                                                                     skipWhiteListCheck)
          case DatabaseType.IMPALA =>
            new ImpalaMetadataParser(connection).getTablesMetadata(configuration.objectType,
                                                                   configuration.schema,
                                                                   configuration.tables,
                                                                   skipWhiteListCheck)
          case _ =>
            Failure(
              new Exception(
                s"Metadata parser for database type: " +
                s"${configuration.databaseType} has not been configured"))
        }
      case Failure(e) =>
        logger.error(s"Failed connecting to: ${configuration.copy(password = "******")}", e)
        throw e
    }
  }

  /**
   * Create a connection to the database
   * @param configuration Database configuration
   * @return
   */
  def getConnection(configuration: DatabaseConf): Try[Connection] = {
    logger.debug("Connecting to database: {}", configuration.copy(password = "******"))
    // Need to register the AS400 manually
    Class.forName("com.ibm.as400.access.AS400JDBCDriver")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    Try(
      DriverManager
        .getConnection(configuration.jdbcUrl, configuration.username, configuration.password))
  }

}
