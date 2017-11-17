package io.phdata.jdbc

import java.sql.JDBCType

import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.util.YamlWrapper
import org.scalatest.FunSuite

class PipewrenchConfigBuilderTest extends FunSuite {
  val initialConfDataPath = "src/test/resources/initialConfData.yml"
  val initialTableDataPath = "src/test/resources/initialTableData.yml"

  test("Create config") {
    val tables = Set(
      Table("REGIONS",
            Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1, 0, -127)),
            Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2, 25, 0))))

    val result =
      PipewrenchConfigBuilder.buildPipewrenchConfig(
        YamlWrapper.read(initialConfDataPath),
        YamlWrapper.read(initialTableDataPath),
        tables)

    // @TODO assert something
    assert(result != null)
  }
}
