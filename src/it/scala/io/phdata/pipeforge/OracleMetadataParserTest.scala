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

package io.phdata.pipeforge

import java.sql.{JDBCType, ResultSet}

import com.whisk.docker.{DockerContainer, DockerReadyChecker}
import io.phdata.pipeforge.common.jdbc._
import io.phdata.pipeforge.jdbc.{DatabaseMetadataParser, OracleMetadataParser}
import io.phdata.pipeforge.jdbc.Implicits._

import scala.util.{Failure, Success}

/**
  * Oracle integration tests
  */
class OracleMetadataParserTest extends DockerTestRunner {

  override val DATABASE = "HR"
  override val USER = "system"
  override val PASSWORD = "oracle"
  override val NO_RECORDS_TABLE = "NO_RECORDS"
  override val TABLE = "REGIONS"
  override val VIEW = "EMP_DETAILS_VIEW"

  override val IMAGE = "wnameless/oracle-xe-11g:latest"
  override val ADVERTISED_PORT = 1521
  override val EXPOSED_PORT = 1521
  override lazy val CONTAINER = DockerContainer(IMAGE)
    .withPorts((ADVERTISED_PORT, Some(EXPOSED_PORT)))
    .withReadyChecker(DockerReadyChecker.LogLineContains("/usr/sbin/startup.sh"))


  override val URL = s"jdbc:oracle:thin:$USER/$PASSWORD@//${CONTAINER.hostname.getOrElse("localhost")}:$EXPOSED_PORT/xe"
  override val DRIVER = "oracle.jdbc.driver.OracleDriver"

  private lazy val DOCKER_CONFIG =
    DatabaseConf(DatabaseType.ORACLE,
      DATABASE,
      URL,
      USER,
      PASSWORD,
      ObjectType.TABLE)

  private lazy val CONNECTION = DatabaseMetadataParser.getConnection(DOCKER_CONFIG).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    startAllOrFail()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAllQuietly()
  }

  test("run query against database") {
    val stmt = CONNECTION.createStatement()
    val results = stmt.executeQuery("SELECT owner, table_name FROM ALL_TABLES where owner = 'HR'").toStream
      .map(x => x.getString(1) + "." + x.getString(2)).toList
    assertResult(7)(results.length)

  }

  test("parse tables metadata") {
    val parser = new OracleMetadataParser(CONNECTION)
    parser.getTablesMetadata(ObjectType.TABLE, DATABASE, Some(List(TABLE))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(TABLE,
            "",
            Set(Column("REGION_ID","", JDBCType.NUMERIC, false, 1, 0, -127)),
            Set(Column("REGION_NAME","", JDBCType.VARCHAR, true, 2, 25, 0)
            )))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("parse views metadata") {
    val parser = new OracleMetadataParser(CONNECTION)
    parser.getTablesMetadata(ObjectType.VIEW, DATABASE, Some(List(VIEW))) match {
      case Success(definitions) =>
        val expected = Set(
          Table(
            VIEW,
            "",
            Set(),
            Set(
              Column("JOB_ID","", JDBCType.VARCHAR, false, 2, 10, 0),
              Column("SALARY","", JDBCType.NUMERIC, true, 9, 8, 2),
              Column("EMPLOYEE_ID","", JDBCType.NUMERIC, false, 1, 6, 0),
              Column("LOCATION_ID","", JDBCType.NUMERIC, true, 5, 4, 0),
              Column("REGION_NAME","", JDBCType.VARCHAR, true, 16, 25, 0),
              Column("COUNTRY_NAME","", JDBCType.VARCHAR, true, 15, 40, 0),
              Column("COUNTRY_ID","", JDBCType.CHAR, true, 6, 2, 0),
              Column("DEPARTMENT_NAME","", JDBCType.VARCHAR, false, 11, 30, 0),
              Column("STATE_PROVINCE","", JDBCType.VARCHAR, true, 14, 25, 0),
              Column("DEPARTMENT_ID","", JDBCType.NUMERIC, true, 4, 4, 0),
              Column("COMMISSION_PCT","", JDBCType.NUMERIC, true, 10, 2, 2),
              Column("JOB_TITLE","", JDBCType.VARCHAR, false, 12, 35, 0),
              Column("FIRST_NAME","", JDBCType.VARCHAR, true, 7, 20, 0),
              Column("CITY","", JDBCType.VARCHAR, false, 13, 30, 0),
              Column("MANAGER_ID","", JDBCType.NUMERIC, true, 3, 6, 0),
              Column("LAST_NAME","", JDBCType.VARCHAR, false, 8, 25, 0)
            )
          ))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }
}
