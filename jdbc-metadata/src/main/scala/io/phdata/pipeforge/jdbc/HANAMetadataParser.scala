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

import io.phdata.pipeforge.jdbc.domain.Column

/**
 * HANA metadata parser implementation
 * @param _connection
 */
class HANAMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT *
       |FROM "$schema"."$table"
       |LIMIT 1
     """.stripMargin

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT TABLE_NAME
       |FROM SYS.TABLES
       |WHERE SCHEMA_NAME = '$schema'
     """.stripMargin

  override def listViewsStatement(schema: String): String =
    s"""
       |SELECT TABLE_NAME
       |FROM SYS.VIEWS
       |WHERE SCHEMA_NAME = '$schema'
     """.stripMargin

  override def tableCommentQuery(schema: String, table: String): String =
    s"""
       |SELECT COMMENTS AS TABLE_COMMENT
       |FROM SYS.TABLES
       |WHERE SCHEMA_NAME = '$schema' AND TABLE_NAME = '$table'
     """.stripMargin

  override def columnCommentsQuery(schema: String, table: String): String =
    s"""
       |SELECT COLUMN_NAME, COMMENTS AS COLUMN_COMMENT
       |FROM SYS.TABLE_COLUMNS
       |WHERE SCHEMA_NAME = '$schema' AND TABLE_NAME = '$table'
       |ORDER BY POSITION
     """.stripMargin
}
