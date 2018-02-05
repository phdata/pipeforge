package io.phdata.pipeforge.config

import io.phdata.pipeforge.jdbc.config.{ DatabaseConf, DatabaseType, ObjectType }
import org.scalatest.FunSuite

class EnvironmentYamlTest extends FunSuite {

  val testFilePath = "src/test/resources/db.yml"

  test("Should parse Yaml file into Environment") {
    val environment = EnvironmentYaml.parseFile(testFilePath)
    val expected = Environment(
      databaseType = "mysql",
      jdbcUrl = "test_jdbc_url",
      schema = "test_schema",
      username = "test_username",
      objectType = "table",
      tables = Some(Seq("test_table1", "test_table2"))
    )
    assertResult(expected)(environment)
  }

  test("Should parse yaml file into DatabaseConf") {
    val testPass     = "test_pass"
    val databaseConf = EnvironmentYaml.getDatabaseConf(testFilePath, testPass)
    val expected = DatabaseConf(
      databaseType = DatabaseType.MYSQL,
      schema = "test_schema",
      jdbcUrl = "test_jdbc_url",
      username = "test_username",
      password = testPass,
      objectType = ObjectType.TABLE,
      tables = Some(Set("test_table1", "test_table2"))
    )
    assertResult(expected)(databaseConf)
  }
}
