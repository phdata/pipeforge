package io.phdata.jdbc

import java.sql._

import com.typesafe.scalalogging.LazyLogging

class MySQLMetadataParser(_connection: Connection)
  extends DatabaseMetadataParser
    with LazyLogging {

  def connection = _connection

  override def getTablesStatement(schema: String, table: String) = s"SELECT * FROM ${schema}.${table} LIMIT 1"

  override def listTablesStatement(schema: String) = "SHOW TABLES"
}
