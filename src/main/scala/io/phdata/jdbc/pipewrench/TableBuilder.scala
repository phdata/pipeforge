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

package io.phdata.jdbc.pipewrench

import java.sql.JDBCType

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Table

/**
  * Builds Pipewrench Table definitions
  */
object TableBuilder extends LazyLogging {
  def buildTablesSection(tableMetadata: Set[Table],
                         initialData: Map[String, Object]) = {
    tableMetadata.toList
      .sortBy(_.name)
      .map(t => buildTable(t) ++ initialData)
  }

  def buildTable(table: Table) = {
    val allColumns = table.primaryKeys ++ table.columns

    Map(
      "id" -> table.name,
      "source" -> Map("name" -> table.name),
      "split_by_column" -> getSplitByColumn(table),
      "destination" -> Map("name" -> table.name.toLowerCase),
      "columns" -> ColumnBuilder.buildColumns(allColumns),
      "primary_keys" -> table.primaryKeys.map(_.name)
    )
  }

  def getSplitByColumn(table: Table) = {
    /*table.primaryKeys.map(x=>{println(x+","+x.dataType)})*/
    table.primaryKeys.find(x => {List (JDBCType.BIGINT,JDBCType.REAL,
      JDBCType.DECIMAL,JDBCType.DOUBLE,JDBCType.FLOAT,JDBCType.INTEGER,
      JDBCType.NUMERIC) contains x.dataType})
      .orElse(table.primaryKeys.headOption)
      .orElse(table.columns.headOption)
      .get
      .name
  }
}
