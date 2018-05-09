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

/**
 * Teradata metadata parser implementation
 *
 * @param _connection
 */
class TeradataMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT top 1 *
       |FROM $schema.$table
     """.stripMargin

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT tablename FROM dbc.tables WHERE tablekind = 'T' and databasename='$schema'
     """.stripMargin

  override def listViewsStatement(schema: String): String =
    s"""
       |SELECT tablename FROM dbc.tables WHERE tablekind = 'T' and databasename='$schema'
     """.stripMargin

  override def tableCommentQuery(schema: String, table: String): String =
    s"""
       |SELECT comment AS TABLE_COMMENT
       |FROM DBC.Tables
       |WHERE DatabaseName = '$schema' AND TableName = '$table'
     """.stripMargin

  override def columnCommentsQuery(schema: String, table: String): String =
    s"""
       |SELECT ColumnName AS COLUMN_NAME, Comment AS COLUMN_COMMENT
       |FROM DBC.Columns
       |WHERE DatabaseName = '$schema' AND TableName = '$table'
     """.stripMargin
}
