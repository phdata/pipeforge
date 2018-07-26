package io.phdata.pipeforge.jdbc

import java.sql.Connection

import io.phdata.pipeforge.common.AppConfiguration
import io.phdata.pipeforge.common.jdbc.{ DatabaseConf, DatabaseType, ObjectType }
import io.phdata.pipeforge.jdbc.Implicits._

import scala.util.{ Failure, Success }

class ImpalaMetadataParser(_connection: Connection)
    extends DatabaseMetadataParser
    with AppConfiguration {

  val queryHiveMetastore: Boolean =
    if (hiveMetastoreUrl.isDefined && hiveMetastoreUsername.isDefined && hiveMetastoreDatabaseType.isDefined && hiveMetastorePassword.isDefined && hiveMetasotreSchema.isDefined)
      true
    else false

  val hiveMetastoreConnection: Option[Connection] = {
    if (queryHiveMetastore) {
      DatabaseMetadataParser.getConnection(
        DatabaseConf(
          databaseType = DatabaseType.withName(hiveMetastoreDatabaseType.get),
          schema = hiveMetasotreSchema.get,
          jdbcUrl = hiveMetastoreUrl.get,
          username = hiveMetastoreUsername.get,
          password = hiveMetastorePassword.get,
          ObjectType.TABLE
        )) match {
        case Success(connection) => Some(connection)
        case Failure(ex) =>
          logger.warn("Failed to connect to Hive metastore", ex)
          None
      }
    } else {
      None
    }
  }

  /**
   * Database connection
   *
   * @return Connection
   */
  override def connection: Connection = _connection

  /**
   * Database specific query that returns a result set containing all tables in the specified schema
   *
   * @param schema Schema or database name
   * @return SQL query listing databases in schema
   */
  override def listTablesStatement(schema: String): String = s"show tables"

  /**
   * Database specific query selecting a single row from the a schema and table
   *
   * @param schema Schema or database name
   * @param table  Table name
   * @return SQL query selecting a single row from table
   */
  override def singleRecordQuery(schema: String, table: String): String =
    s"""
     |SELECT *
     |FROM $schema.$table
     |LIMIT 1
   """.stripMargin

  /**
   * Secondary SQL query for use when tables do not have any records.  This query will left outer join to another
   * table to return a record with all NULL records.
   *
   * @param schema Schema or database name
   * @param table  Table name
   * @return SQL query selecting a single row from a table
   */
  override def joinedSingleRecordQuery(schema: String, table: String): Option[String] = None

  /**
   * Database specific query that returns a result set containing all views in the specified schema
   *
   * @param schema Schema or database name
   * @return
   */
  override def listViewsStatement(schema: String): String = s"show views"

  /**
   * Database specific query that returns the table comment
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The table comment query
   */
  override def tableCommentQuery(schema: String, table: String): Option[String] = None

  /**
   * Queries the Hive metastore for column comments if the configuration is supplied via application.conf properties
   *
   * @param schema Database schema
   * @param table  Database table
   * @return A list containing (column, comment)
   */
  override def getColumnComments(schema: String, table: String): List[(String, Option[String])] =
    hiveMetastoreConnection match {
      case Some(connection) =>
        val stmt  = connection.createStatement()
        val query = columnCommentsQuery(schema, table).get
        logger.debug(s"Getting Impala column comments, query: {}", query)
        val result = stmt
          .executeQuery(query)
          .toStream
          .map(rs => (rs.getString(1), Option(rs.getString(2))))
          .toList
        stmt.close()
        result
      case None =>
        if (!queryHiveMetastore) {
          logger.warn(
            s"Hive Metastore configuations are not supplied via application.conf, cannot query metastore for comments on table: $schema.$table")
          List[(String, Option[String])]()
        } else {
          logger.warn(
            s"Error connecting hive metastore, cannot query metastore for comments on table: $schema.$table")
          List[(String, Option[String])]()
        }
    }

  /**
   * Database specific query that returns the column comments for the specified schema and table
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The column comment query
   */
  override def columnCommentsQuery(schema: String, table: String): Option[String] =
    Some(
      s"""
         |SELECT
         |  COLS.COLUMN_NAME,
         |  CASE WHEN KUDU_DESC.PARAM_VALUE IS NOT NULL THEN KUDU_DESC.PARAM_VALUE
         |    ELSE COLS.COMMENT end as COLUMN_COMMENT
         |FROM DBS
         |  INNER JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID
         |  INNER JOIN SDS ON TBLS.SD_ID = SDS.SD_ID
         |  INNER JOIN COLUMNS_V2 COLS ON SDS.CD_ID = COLS.CD_ID
         |  LEFT OUTER JOIN TABLE_PARAMS KUDU_DESC ON TBLS.TBL_ID = KUDU_DESC.TBL_ID AND COLS.COLUMN_NAME = KUDU_DESC.PARAM_KEY
         |WHERE DBS.NAME = '$schema' AND TBLS.TBL_NAME = '$table'
       """.stripMargin
    )
}
