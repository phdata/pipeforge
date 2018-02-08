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

package io.phdata.pipewrench

import java.io.FileWriter
import java.sql.JDBCType

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.jdbc.domain.{ Column, Table }
import io.phdata.pipewrench.domain.{ ColumnYaml, DataType, TableYaml }
import io.phdata.pipewrench.domain.TableYamlProtocol._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._

object Pipewrench extends LazyLogging with DefaultYamlProtocol {

  def buildYaml(tables: Set[Table]) = buildTables(tables).toYaml

  def buildYaml(tables: Set[Table], outputPath: String): Unit = {
    val yaml = buildYaml(tables)
    logger.debug(s"Parsed tables yml: $yaml")
    writeYamlFile(yaml, outputPath)
  }

  private def writeYamlFile(yaml: YamlValue, path: String): Unit = {
    val fw = new FileWriter(path)
    logger.debug(s"Writing file: $path")
    fw.write(yaml.toString)
    fw.close()
  }

  private def buildTables(tables: Set[Table]): Seq[TableYaml] =
    tables.toList
      .sortBy(_.name)
      .map { table =>
        logger.debug(s"Table definition: $table")
        val allColumns = table.primaryKeys ++ table.columns

        TableYaml(
          table.name,
          Map("name" -> table.name),
          Map("name" -> table.name),
          getSplitByColumn(table),
          table.primaryKeys.toList.sortBy(_.index).map(_.name),
          buildColumns(allColumns)
        )
      }

  private def buildColumns(columns: Set[Column]): Seq[ColumnYaml] =
    columns.toList
      .sortBy(_.index)
      .map { column =>
        val dataType = DataType.mapDataType(column)
        logger.debug(s"Column definition: $column, mapped dataType: $dataType")
        val columnYaml = ColumnYaml(column.name, dataType)
        if (dataType == DataType.DECIMAL.toString) {
          logger.trace("Found decimal value: {}", column)
          columnYaml.copy(scale = Some(column.scale), precision = Some(column.precision))
        } else {
          columnYaml
        }
      }

  def getSplitByColumn(table: Table) = {
    val jdbc_numerics = List(JDBCType.BIGINT,
                             JDBCType.REAL,
                             JDBCType.DECIMAL,
                             JDBCType.DOUBLE,
                             JDBCType.FLOAT,
                             JDBCType.INTEGER,
                             JDBCType.NUMERIC)
    table.primaryKeys
      .find(x => { jdbc_numerics contains x.dataType })
      .orElse(table.primaryKeys.headOption)
      .orElse(table.columns.find(x => { jdbc_numerics contains x.dataType }))
      .orElse(table.columns.headOption)
      .get
      .name
  }
}
