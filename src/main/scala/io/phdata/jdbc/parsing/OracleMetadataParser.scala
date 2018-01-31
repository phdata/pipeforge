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
  * Oracle metadata parser implementation
  * @param _connection
  */
class OracleMetadataParser(_connection: Connection) extends DatabaseMetadataParser {

  def connection = _connection

  override def singleRecordQuery(schema: String, table: String) =
    s"""
       |SELECT *
       |FROM $schema.$table
       |WHERE ROWNUM = 1
     """.stripMargin

  override def listTablesStatement(schema: String) =
    s"""
       |SELECT table_name
       |FROM ALL_TABLES
       |WHERE owner = '$schema'
     """.stripMargin

  override def listViewsStatement(schema: String) =
    s"""
       |SELECT view_name
       |FROM ALL_VIEWS
       |WHERE owner = '$schema'
     """.stripMargin

}
