package io.phdata.jdbc.parsing

import java.sql.{Connection, ResultSet, ResultSetMetaData}

import io.phdata.jdbc.domain.Column

import scala.util.{Failure, Success, Try}

/**
  * HANA metadata parser implementation
  * @param _connection
  */
class HANAMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

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

  override def primaryKeys(schema: String, table: String, columns: Set[Column]): Set[Column] = {
    //val rs: ResultSet = metadata.getPrimaryKeys(schema, schema, table)
    logger.debug("Gathering primary keys from JDBC metadata")
    //val pks = results(rs) { record =>
    //  record.getString("COLUMN_NAME") -> record.getInt("KEY_SEQ")
    //}.toMap

    //mapPrimaryKeyToColumn(pks, columns)
    Set()

  }
  override def getColumnDefinitions(schema: String,
                                    table: String): Try[Set[Column]] = {
    val query = singleRecordQuery(schema, table)
    logger.debug(s"Gathering column definitions for $schema.$table, query: {}", query)
    results(newStatement.executeQuery(query))(_.getMetaData).toList.headOption match {
      case Some(metaData) =>
        val rsMetadata = metaData.asInstanceOf[com.sap.db.jdbc.trace.ResultSetMetaData]
        Success(mapMetaDataToColumn(metaData, rsMetadata))
      case None =>
        Failure(new Exception(s"$table does not contain any records, cannot provide column definitions"))
    }
  }
}
