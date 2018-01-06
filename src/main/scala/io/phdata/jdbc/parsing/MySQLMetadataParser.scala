package io.phdata.jdbc.parsing

import java.sql.{Connection, ResultSetMetaData}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

/**
  * MySQL metadata parser implementation
  * @param _connection
  */
class MySQLMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT *
       |FROM $schema.$table
       |LIMIT 1
     """.stripMargin

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT TABLE_NAME
       |FROM INFORMATION_SCHEMA.TABLES
       |WHERE TABLE_SCHEMA = '$schema' AND TABLE_TYPE = 'BASE TABLE'
     """.stripMargin


  override def listViewsStatement(schema: String): String =
    s"""
       |SELECT TABLE_NAME
       |FROM INFORMATION_SCHEMA.VIEWS
       |WHERE TABLE_SCHEMA = '$schema'
     """.stripMargin

  override def getColumnDefinitions(schema: String,
                                    table: String): Set[Column] = {
    val query = singleRecordQuery(schema, table)
    logger.debug(s"Gathering column definitions for $schema.$table, query: {}", query)
    val metaData: ResultSetMetaData = results(newStatement.executeQuery(query))(_.getMetaData).toList.head
    val rsMetadata = metaData.asInstanceOf[com.mysql.cj.jdbc.result.ResultSetMetaData]
    mapMetaDataToColumn(metaData, rsMetadata)
  }
}
