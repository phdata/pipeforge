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

import java.sql.JDBCType

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.Column

/**
 * Builds Pipewrench column definitions
 */
object ColumnBuilder extends LazyLogging {

  def buildColumns(columns: Set[Column]): Seq[Map[String, Any]] = {
    logger.debug(s"Building Columns: {}", columns)
    columns.toList
      .sortBy(_.index)
      .map(buildColumn)
  }

  def buildColumn(column: Column): Map[String, Any] = {
    val dataType = mapDataType(column)
    val map =
      Map("name" -> column.name, "datatype" -> dataType, "comment" -> "")

    logger.debug(s"Column definition: $column, mapped dataType: $dataType")
    if (dataType == DataType.DECIMAL.toString) {
      logger.trace("Found decimal value: {}", column)
      map + ("scale" -> column.scale) + ("precision" -> column.precision)
    } else {
      map
    }
  }

  def mapDataType(column: Column): String =
    column match {
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s > 0            => DataType.DECIMAL.toString
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 19 => DataType.DECIMAL.toString
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 10 => DataType.BIG_INT.toString
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 5  => DataType.INTEGER.toString
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 3  => DataType.SHORT.toString
      case _                                                           => column.dataType.toString
    }
}

object DataType extends Enumeration {
  val BOOLEAN = Value("BOOLEAN")
  val DECIMAL = Value("DECIMAL")
  val BIG_INT = Value("BIGINT")
  val INTEGER = Value("INTEGER")
  val SHORT   = Value("SHORT")
}
// map column java for numeric types
