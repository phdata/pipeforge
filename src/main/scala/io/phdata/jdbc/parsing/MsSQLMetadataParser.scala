package io.phdata.jdbc.parsing

import java.sql.{Connection, JDBCType, ResultSetMetaData}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

class MsSQLMetadataParser(_connection: Connection) extends DatabaseMetadataParser with LazyLogging{

  override def connection = _connection

  override def listTablesStatement(schema: String) = "SELECT * FROM sys.tables"

  override def singleRecordQuery(schema: String, table: String) = s"SELECT TOP 1 * FROM $table"

  override def listViewsStatement(schema: String) = ???

  override def getColumnDefinitions(schema: String, table: String): Set[Column] = {
    def asBoolean(i: Int) = if (i == 0) false else true

    val query = singleRecordQuery(schema, table)
    logger.debug("Executing query: {}", query)
    val metaData: ResultSetMetaData =
      results(newStatement.executeQuery(query))(_.getMetaData).toList.head
    val rsMetadata = metaData.asInstanceOf[net.sourceforge.jtds.jdbc.JtdsResultSetMetaData]
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

    val pks = results(newStatement.executeQuery(query)) { record =>
      record.getString("COLUMN_NAME") -> record.getInt("ORDINAL_POSITION")
    }.toMap

    mapPrimaryKeyToColumn(pks, columns)
  }
}
