package io.phdata.pipeforge.rest

import io.phdata.pipeforge.jdbc.config.{ DatabaseConf, DatabaseType, ObjectType }
import io.phdata.pipeforge.rest.domain.{ Environment, YamlProtocol }
import io.phdata.pipeforge.rest.domain.Implicits._

import org.scalatest.FunSuite

class YamlProtocolTest extends FunSuite with YamlProtocol {

  val testFilePath = "rest-api/src/test/resources/db.yml"

  test("YamlProtocol Trait should parse a file into Environment") {
    val environment = parseFile(testFilePath)
    val expected = Environment(
      "test_name",
      "test_group",
      "mysql",
      "test_schema",
      "test_jdbc_url",
      "test_username",
      "table",
      Map("key1" -> "val1"),
      "test_hdfs_path",
      "test_hadoop_user",
      "test_password_file",
      "test_destination_database",
      Some(List("test_table1", "test_table2"))
    )
    assertResult(expected)(environment)
  }

  test("Should parse yaml file into DatabaseConf") {
    val testPass     = "test_pass"
    val environment  = parseFile(testFilePath)
    val databaseConf = environment.toDatabaseConfig(testPass)
    val expected = DatabaseConf(
      databaseType = DatabaseType.MYSQL,
      schema = "test_schema",
      jdbcUrl = "test_jdbc_url",
      username = "test_username",
      password = testPass,
      objectType = ObjectType.TABLE,
      tables = Some(List("test_table1", "test_table2"))
    )
    assertResult(expected)(databaseConf)
  }
}
