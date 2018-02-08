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

package io.phdata.pipewrench.domain

import java.sql.JDBCType

import io.phdata.pipeforge.jdbc.domain.Column
import org.scalatest.FunSuite

/**
 * ColumnBuilder unit tests
 */
class DataTypeMappingTest extends FunSuite {
  test("map decimal") {
    val column = Column("col1", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    assertResult(DataType.DECIMAL.toString)(DataType.mapDataType(column))
  }

  test("map big decimal") {
    val column = Column("col1", JDBCType.NUMERIC, nullable = false, 0, 19, 0)
    assertResult(DataType.BIG_INT.toString)(DataType.mapDataType(column))
  }

  test("map int") {
    val column = Column("col1", JDBCType.NUMERIC, nullable = false, 0, 9, 0)
    assertResult(DataType.INTEGER.toString)(DataType.mapDataType(column))
  }

  test("map short") {
    val column = Column("col1", JDBCType.NUMERIC, nullable = false, 0, 5, 0)
    assertResult(DataType.SHORT.toString)(DataType.mapDataType(column))
  }

  test("map boolean") {
    val column = Column("col1", JDBCType.BOOLEAN, nullable = false, 0, 0, 0)
    assertResult(DataType.BOOLEAN.toString)(DataType.mapDataType(column))
  }
}
