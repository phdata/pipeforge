package io.phdata.jdbc.parsing

import java.sql.{Connection, ResultSetMetaData}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

/**
  * Oracle metadata parser implementation
  * @param _connection
  */
class OracleMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT *
       |FROM $schema.$table
       |WHERE ROWNUM = 1
     """.stripMargin

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT table_name
       |FROM ALL_TABLES
       |WHERE owner = '$schema'
     """.stripMargin

  override def listViewsStatement(schema: String) =
    s"""
       |SELECT view_name
       |FROM ALL_VIEWS
       |WHERE owner = '$schema'
     """.stripMargin

  override def getColumnDefinitions(schema: String,
                                    table: String): Set[Column] = {
    val query = singleRecordQuery(schema, table)
    logger.debug(s"Gathering column definitions for $schema.$table, query: {}", query)
    val metaData: ResultSetMetaData = results(newStatement.executeQuery(query))(_.getMetaData).toList.head
    val rsMetadata = metaData.asInstanceOf[oracle.jdbc.OracleResultSetMetaData]
    mapMetaDataToColumn(metaData, rsMetadata)
  }
}
