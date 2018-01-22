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

import io.phdata.jdbc.domain.Column
import io.phdata.jdbc.domain.Table
import org.scalatest.FunSuite

/**
  * TableBuilder unit tests for getSplitByColumn
  */
class TableBuilderTest extends FunSuite{
  test("all numeric PKs") {
    val col1 = Column("col1", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(col1,col2,col3), Set(col1,col2,col3,col4,col5))
    assertResult("col1")(TableBuilder.getSplitByColumn(table))
  }

  test("numeric and varchar PKs") {
    val col1 = Column("col1", JDBCType.VARCHAR, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.NUMERIC, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(col1,col2,col3), Set(col1,col2,col3,col4,col5))
    assertResult("col2")(TableBuilder.getSplitByColumn(table))
  }

  test("decimal and non-numerics PKs") {
    val col1 = Column("col1", JDBCType.VARCHAR, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.BOOLEAN, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.DECIMAL, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(col1,col2,col3), Set(col1,col2,col3,col4,col5))
    assertResult("col3")(TableBuilder.getSplitByColumn(table))
  }

  test("different numerics PKs") {
    val col1 = Column("col1", JDBCType.VARCHAR, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.BIGINT, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.FLOAT, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(col1,col2,col3), Set(col1,col2,col3,col4,col5))
    assertResult("col2")(TableBuilder.getSplitByColumn(table))
  }

  test("no numerics in PK") {
    val col1 = Column("col1", JDBCType.VARCHAR, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.BOOLEAN, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.CLOB, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(col1,col2,col3), Set(col1,col2,col3,col4,col5))
    assertResult("col1")(TableBuilder.getSplitByColumn(table))
  }

  test("no PK") {
    val col1 = Column("col1", JDBCType.VARCHAR, nullable = false, 0, 10, 4)
    val col2 = Column("col2", JDBCType.BOOLEAN, nullable = false, 0, 10, 4)
    val col3 = Column("col3", JDBCType.CLOB, nullable = false, 0, 10, 4)
    val col4 = Column("col4", JDBCType.VARCHAR, nullable = true, 0, 10, 4)
    val col5 = Column("col5", JDBCType.NUMERIC, nullable = true, 0, 10, 4)
    val table = Table("tbl1", Set(), Set(col1,col2,col3,col4,col5))
    assertResult("col2")(TableBuilder.getSplitByColumn(table))
  }

}
