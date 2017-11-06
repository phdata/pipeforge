package io.phdata.jdbc

import java.sql.{Connection, Statement}

import com.typesafe.scalalogging.LazyLogging

class OracleMetadataParser(_connection: Connection)
    extends DatabaseMetadataParser
    with LazyLogging {

  def connection = _connection

  override def getTablesStatement(schema: String, table: String) = s"SELECT * FROM ${table} WHERE ROWNUM = 1"

  override def listTablesStatement(schema: String) = s"SELECT table_name FROM ALL_TABLES WHERE owner = '$schema'"
}
