package io.phdata.jdbc.parsing

import java.sql.{Connection, ResultSetMetaData}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

class MsSQLMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  override def connection = _connection

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT TABLE_NAME
       |FROM information_schema.tables
       |WHERE TABLE_CATALOG = '$schema' AND TABLE_TYPE = 'BASE TABLE'
     """.stripMargin

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT TOP 1 *
       |FROM $table
     """.stripMargin

  override def listViewsStatement(schema: String) =
    s"""
       |SELECT TABLE_NAME
       |FROM information_schema.views
       |WHERE TABLE_CATALOG = '$schema'
     """.stripMargin

  override def getColumnDefinitions(schema: String, table: String): Set[Column] = {
    val query = singleRecordQuery(schema, table)
    logger.debug("Executing query: {}", query)
    val metaData: ResultSetMetaData = results(newStatement.executeQuery(query))(_.getMetaData).toList.head
    val rsMetadata = metaData.asInstanceOf[com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData]
    mapMetaDataToColumn(metaData, rsMetadata)
  }

  /**
    * NOTE: connection.getMetaData.getPrimaryKeys does not return primary keys for MsSQL, hence why this is here
    * @param schema
    * @param table
    * @param columns
    * @return
    */
  override def primaryKeys(schema: String,
                           table: String,
                           columns: Set[Column]): Set[Column] = {
    val query =
      s"""
         |SELECT COLUMN_NAME, ORDINAL_POSITION
         |FROM INFORMATION_SCHEMA.key_column_usage c
         |  JOIN INFORMATION_SCHEMA.table_constraints t ON c.TABLE_NAME = t.TABLE_NAME
         |WHERE t.TABLE_CATALOG = '$schema' AND t.TABLE_NAME = '$table' AND CONSTRAINT_TYPE = 'PRIMARY KEY';
       """.stripMargin

    val pks = results(newStatement.executeQuery(query)) {
      record =>
        record.getString("COLUMN_NAME") -> record.getInt("ORDINAL_POSITION")
    }.toMap

    mapPrimaryKeyToColumn(pks, columns)
  }
}
