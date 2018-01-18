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

package io.phdata.jdbc.util

import java.sql.JDBCType

import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.pipewrench.TableBuilder
import org.scalatest.FunSuite


/**
  * YamlWrapper unit tests
  */
class YamlWrapperTest extends FunSuite {
  test("write yaml") {
    val primaryKey = Column("id", JDBCType.BIGINT, nullable = false, 1, 9, 0)
    val column = Column("name", JDBCType.VARCHAR, nullable = true, 2, 0, 0)
    val table =  Table("test", Set(primaryKey), Set(column))
    val data = Map("tables" -> TableBuilder.buildTablesSection(Set(table), Map.empty))
//    val data = Map("key" -> "value",
//      "list" ->
//        Seq(Map("one" -> "two")
//          , Map("three" -> "four")))


    YamlWrapper.write(data, "target/test.yml")
  }

  test("read yaml") {
    val data = Map("key" -> "value",
      "list" ->
        Seq(Map("one" -> "two")
          , Map("three" -> "four")))


    YamlWrapper.write(data, "target/test.yml")
    val str = YamlWrapper.read("target/test.yml")
    assert(str != null)
  }
}
