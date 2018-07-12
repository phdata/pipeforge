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
 * MySQL metadata parser implementation
 * @param _connection
 */
class MySQLMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT *
       |FROM $schema.$table
       |LIMIT 1
     """.stripMargin

  override def joinedSingleRecordQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT t.*
       |FROM INFORMATION_SCHEMA.TABLES AS d
       |  LEFT OUTER JOIN $schema.$table AS t ON 1=1
       |LIMIT 1
     """.stripMargin)

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

  override def tableCommentQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT TABLE_COMMENT
       |FROM INFORMATION_SCHEMA.TABLES
       |WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'
     """.stripMargin)

  override def columnCommentsQuery(schema: String, table: String): Option[String] =
    Some(s"""
       |SELECT COLUMN_NAME, COLUMN_COMMENT
       |FROM INFORMATION_SCHEMA.COLUMNS
       |WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'
     """.stripMargin)

}
