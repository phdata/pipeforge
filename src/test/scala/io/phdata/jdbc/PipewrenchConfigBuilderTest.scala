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

package io.phdata.jdbc

import java.sql.JDBCType

import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.util.YamlWrapper
import org.scalatest.FunSuite

/**
  * PipewrenchConfigBuilder unit tests
  */
class PipewrenchConfigBuilderTest extends FunSuite {
  val initialTableDataPath = "src/test/resources/initialTableData.yml"

  test("Create config") {
    val tables = Set(
      Table("REGIONS",
            Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1, 0, -127)),
            Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2, 25, 0))))

    val result = Map("tables" ->
      PipewrenchConfigBuilder.buildPipewrenchConfig(tables, YamlWrapper.read(initialTableDataPath)))

    // @TODO assert something
    assert(result != null)
  }
}
