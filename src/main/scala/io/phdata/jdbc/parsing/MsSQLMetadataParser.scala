/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.jdbc.parsing

import java.sql.{Connection, ResultSetMetaData}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Column

import scala.util.{Failure, Success, Try}

/**
  * Microsoft SQL Server metadata parser implementation
  * @param _connection
  */
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

  override def getColumnDefinitions(schema: String, table: String): Try[Set[Column]] = {
    val query = singleRecordQuery(schema, table)
    logger.debug(s"Gathering column definitions for $schema.$table, query: {}", query)
    results(newStatement.executeQuery(query))(_.getMetaData).toList.headOption match {
      case Some(metaData) =>
        val rsMetadata = metaData.asInstanceOf[com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData]
        Success(mapMetaDataToColumn(metaData, rsMetadata))
      case None =>
        Failure(new Exception(s"$table does not contain any records, cannot provide column definitions"))
    }

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

    logger.debug(s"Gathering primary keys for $schema.$table, query: {}", query)
    val pks = results(newStatement.executeQuery(query)) {
      record =>
        record.getString("COLUMN_NAME") -> record.getInt("ORDINAL_POSITION")
    }.toMap

    mapPrimaryKeyToColumn(pks, columns)
  }
}
