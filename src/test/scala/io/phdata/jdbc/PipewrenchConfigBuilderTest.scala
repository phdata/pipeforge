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
