package io.phdata.jdbc

import java.sql.{JDBCType, ResultSet}

import com.whisk.docker.{DockerContainer, DockerReadyChecker}
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, OracleMetadataParser}

import scala.util.{Failure, Success}

class OracleMetadataParserTest extends DockerTestRunner {

  private lazy val DATABASE = "HR"
  private lazy val USER = "system"
  private lazy val PASSWORD = "oracle"
  private lazy val TABLE = "REGIONS"
  private lazy val VIEW = "EMP_DETAILS_VIEW"

  override val image = "wnameless/oracle-xe-11g:latest"
  override val advertisedPort = 1521
  override val exposedPort = 1521
  override val container = DockerContainer(image)
    .withPorts((advertisedPort, Some(exposedPort)))
    .withReadyChecker(DockerReadyChecker.LogLineContains("/usr/sbin/startup.sh"))


  private lazy val URL = s"jdbc:oracle:thin:$USER/$PASSWORD@//${container.hostname.getOrElse("localhost")}:$exposedPort/xe"
  private lazy val DRIVER = "oracle.jdbc.driver.OracleDriver"

  private lazy val dockerConfig =
    new DatabaseConf(DatabaseType.ORACLE,
      DATABASE,
      URL,
      USER,
      PASSWORD,
      ObjectType.TABLE)

  private lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    startAllOrFail()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAllQuietly()
  }

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet =
      stmt.executeQuery(
        "SELECT owner, table_name FROM ALL_TABLES where owner = 'HR'")
    val results =
      getResults(rs)(x => x.getString(1) + "." + x.getString(2)).toList
    assertResult(7)(results.length)

  }

  test("parse tables metadata") {
    val parser = new OracleMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.TABLE, DATABASE, Some(Set(TABLE))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(TABLE,
            Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1, 0, -127)),
            Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2, 25, 0)
            )))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("parse views metadata") {
    val parser = new OracleMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.VIEW, DATABASE, Some(Set(VIEW))) match {
      case Success(definitions) =>
        val expected = Set(
          Table(
            VIEW,
            Set(),
            Set(
              Column("JOB_ID", JDBCType.VARCHAR, false, 2, 10, 0),
              Column("SALARY", JDBCType.NUMERIC, true, 9, 8, 2),
              Column("EMPLOYEE_ID", JDBCType.NUMERIC, false, 1, 6, 0),
              Column("LOCATION_ID", JDBCType.NUMERIC, true, 5, 4, 0),
              Column("REGION_NAME", JDBCType.VARCHAR, true, 16, 25, 0),
              Column("COUNTRY_NAME", JDBCType.VARCHAR, true, 15, 40, 0),
              Column("COUNTRY_ID", JDBCType.CHAR, true, 6, 2, 0),
              Column("DEPARTMENT_NAME", JDBCType.VARCHAR, false, 11, 30, 0),
              Column("STATE_PROVINCE", JDBCType.VARCHAR, true, 14, 25, 0),
              Column("DEPARTMENT_ID", JDBCType.NUMERIC, true, 4, 4, 0),
              Column("COMMISSION_PCT", JDBCType.NUMERIC, true, 10, 2, 2),
              Column("JOB_TITLE", JDBCType.VARCHAR, false, 12, 35, 0),
              Column("FIRST_NAME", JDBCType.VARCHAR, true, 7, 20, 0),
              Column("CITY", JDBCType.VARCHAR, false, 13, 30, 0),
              Column("MANAGER_ID", JDBCType.NUMERIC, true, 3, 6, 0),
              Column("LAST_NAME", JDBCType.VARCHAR, false, 8, 25, 0)
            )
          ))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }
}
