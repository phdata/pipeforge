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

package io.phdata.pipeforge.jdbc

import java.sql.Connection

import io.phdata.pipeforge.common.jdbc.Column
import io.phdata.pipeforge.jdbc.Implicits._

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
       |WHERE TABLE_SCHEMA = '$schema' AND TABLE_TYPE = 'BASE TABLE'
     """.stripMargin

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT TOP 1 *
       |FROM \"$table\"
     """.stripMargin

  override def joinedSingleRecordQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT TOP 1 t.*
       |FROM INFORMATION_SCHEMA.TABLES AS d
       |  LEFT OUTER JOIN \"$schema\".\"$table\" AS t ON 1=1
     """.stripMargin)

  override def listViewsStatement(schema: String) =
    s"""
       |SELECT TABLE_NAME
       |FROM information_schema.views
       |WHERE TABLE_SCHEMA = '$schema'
     """.stripMargin

  /**
   * NOTE: connection.getMetaData.getPrimaryKeys does not return primary keys for MsSQL, hence why this is here
   * @param schema
   * @param table
   * @param columns
   * @return
   */
  override def primaryKeys(schema: String, table: String, columns: Set[Column]): Set[Column] = {
    val query =
      s"""
         |SELECT COLUMN_NAME, ORDINAL_POSITION
         |FROM INFORMATION_SCHEMA.key_column_usage c
         |  JOIN INFORMATION_SCHEMA.table_constraints t ON c.TABLE_NAME = t.TABLE_NAME
         |WHERE t.TABLE_SCHEMA = '$schema' AND t.TABLE_NAME = '$table' AND CONSTRAINT_TYPE = 'PRIMARY KEY';
       """.stripMargin

    logger.debug(s"Gathering primary keys for $schema.$table, query: {}", query)
    val stmt = connection.createStatement()
    val pks = stmt
      .executeQuery(query)
      .toStream
      .map { record =>
        record.getString("COLUMN_NAME") -> record.getInt("ORDINAL_POSITION")
      }
      .toMap

    val result = mapPrimaryKeyToColumn(pks, columns)
    stmt.close()
    result
  }

  override def tableCommentQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT CAST(ep.value AS NVARCHAR(255)) AS TABLE_COMMENT
       |FROM sys.objects objects
       |   INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id
       |   CROSS APPLY fn_listextendedproperty(default,
       |                                    'SCHEMA', schemas.name,
       |                                    'TABLE', objects.name, null, null) ep
       |WHERE objects.name NOT IN ('sysdiagrams')
       |  AND objects.name = '$table'
       |  AND schemas.name = '$schema'
       |ORDER BY objects.name
     """.stripMargin)

  override def columnCommentsQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT columns.name AS COLUMN_NAME, CAST(ep.value AS NVARCHAR(255)) AS COLUMN_COMMENT
       |FROM sys.objects objects
       |  INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id
       |  INNER JOIN sys.columns columns ON objects.object_id = columns.object_id
       |  CROSS APPLY fn_listextendedproperty(default,
       |                  'SCHEMA', schemas.name,
       |                  'TABLE', objects.name, 'COLUMN', columns.name) ep
       |WHERE objects.name = '$table'
       |  AND schemas.name = '$schema'
       |ORDER BY objects.name, columns.column_id
     """.stripMargin)

}
