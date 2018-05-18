package io.phdata.pipeforge.jdbc
import java.sql.Connection

class SybaseMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

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
       |SELECT name
       |FROM sysobjects
       |WHERE type = 'U'
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
     |SELECT TOP 1 *
     |FROM $schema.$table
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
       |SELECT TOP 1 t.*
       |FROM sysobjects AS d
       |  LEFT OUTER JOIN $schema.$table AS t ON 1=1
     """.stripMargin

  /**
   * Database specific query that returns a result set containing all views in the specified schema
   *
   * @param schema Schema or database name
   * @return
   */
  override def listViewsStatement(schema: String): String =
    s"""
       |SELECT name
       |FROM sysobjects
       |WHERE type = 'V'
     """.stripMargin

  /**
   * Database specific query that returns the table comment
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The table comment query
   */
  override def tableCommentQuery(schema: String, table: String): String =
    s"""
       |SELECT text
       |FROM sysobjects AS o
       |  INNER JOIN syscomments AS c ON o.id = c.id
       |WHERE o.name = '$table'
     """.stripMargin

  /**
   * Database specific query that returns the column comments for the specified schema and table
   *
   * @param schema Database schema
   * @param table  Database table
   * @return The column comment query
   */
  override def columnCommentsQuery(schema: String, table: String): String =
    s"""
       |SELECT col.name AS COLUMN_NAME, com.text AS COLUMN_COMMENT
       |FROM sysobjects AS obj
       |  INNER JOIN syscolumns AS col ON col.id = obj.id
       |  INNER JOIN syscomments AS com ON com.id = obj.id AND com.colid = col.colid
       |WHERE o.name = '$table'
     """.stripMargin

}
