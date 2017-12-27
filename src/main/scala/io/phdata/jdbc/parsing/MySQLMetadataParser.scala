package io.phdata.jdbc.parsing

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

class MySQLMetadataParser(_connection: Connection)
    extends DatabaseMetadataParser
    with LazyLogging {

  def connection = _connection

  override def listTablesStatement(schema: String) = "SHOW TABLES"

  override def getColumnDefinitions(schema: String,
                                    table: String): Set[Column] = {
    def asBoolean(i: Int) = if (i == 0) false else true

    val query = singleRecordQuery(schema, table)
    logger.debug("Executing query: {}", query)
    val metaData: ResultSetMetaData =
      results(newStatement.executeQuery(query))(_.getMetaData).toList.head
    val rsMetadata = metaData.asInstanceOf[com.mysql.cj.jdbc.result.ResultSetMetaData]
    (1 to metaData.getColumnCount).map { i =>
      Column(
        metaData.getColumnName(i),
        JDBCType.valueOf(rsMetadata.getColumnType(i)),
        asBoolean(metaData.isNullable(i)),
        i,
        metaData.getPrecision(i),
        metaData.getScale(i)
      )
    }.toSet
  }

  override def singleRecordQuery(schema: String, table: String) =
    s"SELECT * FROM $schema.$table LIMIT 1"

  override def listViewsStatement(schema: String): String =
    throw new NotImplementedError()
}
