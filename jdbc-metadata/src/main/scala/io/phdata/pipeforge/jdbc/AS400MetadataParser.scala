package io.phdata.pipeforge.jdbc
import java.sql.Connection

class AS400MetadataParser(_connection: Connection) extends DatabaseMetadataParser {

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
  override def listTablesStatement(schema: String): String =
    s"""
       |SELECT TABLE_NAME
       |FROM QSYS2.SYSTABLES
       |WHERE (TABLE_TYPE = 'T' OR TABLE_TYPE = 'P' OR TABLE_TYPE = 'L') AND TABLE_SCHEMA = '$schema'
     """.stripMargin

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
  override def joinedSingleRecordQuery(schema: String, table: String): String =
    s"""
       |SELECT t.*
       |FROM QSYS2.SYSTABLES d
       |LEFT OUTER JOIN $schema.$table t ON 1=1
       |LIMIT 1
     """.stripMargin

  /**
   * Database specific query that returns a result set containing all views in the specified schema
   *
   * @param schema Schema or database name
   * @return
   */
  override def listViewsStatement(schema: String): String =
    s"""
       |SELECT TABLE_NAME
       |FROM QSYS2.SYSTABLES
       |WHERE TABLE_TYPE = 'V' AND TABLE_SCHEMA = '$schema'
     """.stripMargin

  /**
   * Database specific query that returns the table comment
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The table comment query
   */
  override def tableCommentQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT COALESCE(LONG_COMMENT, TABLE_TEXT) AS TABLE_COMMENT
       |FROM QSYS2.SYSTABLES
       |WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'
     """.stripMargin)

  /**
   * Database specific query that returns the column comments for the specified schema and table
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The column comment query
   */
  override def columnCommentsQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT COLUMN_NAME, COALESCE(COLUMN_TEXT, LONG_COMMENT, COLUMN_HEADING) AS COLUMN_COMMENT
       |FROM QSYS2.SYSCOLUMNS
       |WHERE TABLE_NAME = '$table'
       |ORDER BY ORDINAL_POSITION
     """.stripMargin)
}
