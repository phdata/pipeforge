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
import io.phdata.pipeforge.jdbc.{DatabaseMetadataParser, MsSQLMetadataParser, MySQLMetadataParser}
import io.phdata.pipeforge.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.pipeforge.jdbc.domain.{Column, Table}
import io.phdata.pipeforge.jdbc.Implicits._

import scala.util.{Failure, Success}

/**
  * MySQL integration tests
  */
class MySQLMetadataParserTest extends DockerTestRunner {

  private lazy val ROOT_PASS = "root"
  // Database Properties
  override val DATABASE = "it_test"
  override val USER = "it_user"
  override val PASSWORD = "it_test"
  override val NO_RECORDS_TABLE = "no_records"
  override val TABLE = "it_table"
  override val VIEW = "it_view"
  // Container Properties
  override val IMAGE = "mysql"
  override val ADVERTISED_PORT = 3306
  override val EXPOSED_PORT = 3306
  override lazy val CONTAINER = DockerContainer(IMAGE)
    .withPorts((ADVERTISED_PORT, Some(EXPOSED_PORT)))
    .withEnv(
      s"MYSQL_ROOT_PASSWORD=$ROOT_PASS",
      s"MYSQL_DATABASE=$DATABASE",
      s"MYSQL_USER=$USER",
      s"MYSQL_PASSWORD=$PASSWORD")
    .withReadyChecker(DockerReadyChecker.LogLineContains("mysqld: ready for connections"))

  override val URL = s"jdbc:mysql://${CONTAINER.hostname.getOrElse("localhost")}:$EXPOSED_PORT/$DATABASE"
  override val DRIVER = "com.mysql.jdbc.Driver"

  private lazy val DOCKER_CONFIG = new DatabaseConf(DatabaseType.MYSQL,
    DATABASE,
    URL,
    USER,
    PASSWORD,
    ObjectType.TABLE)

  private lazy val CONNECTION = DatabaseMetadataParser.getConnection(DOCKER_CONFIG).get


  override def beforeAll(): Unit = {
    super.beforeAll()
    startAllOrFail()
    Thread.sleep(5000)
    createTestTable()
    createEmptyTable()
    insertTestData()
    createTestView()
  }

  override def afterAll(): Unit = {
    stopAllQuietly()
    super.afterAll()
  }
  test("run query against database") {
    val stmt = CONNECTION.createStatement()
    val results = stmt.executeQuery("SELECT table_name FROM information_schema.tables").toStream
      .map(_.getString(1)).toList
    assertResult(64)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MySQLMetadataParser(CONNECTION)
    parser.getTablesMetadata(ObjectType.TABLE, DATABASE, None) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(TABLE,
            Set(Column("primary_key", JDBCType.INTEGER, false, 1, 11, 0)),
            Set(Column("b_boolean",JDBCType.BIT,true,7,1,0),
              Column("i_int",JDBCType.INTEGER,true,5,11,0),
              Column("b_bigint",JDBCType.BIGINT,true,6,20,0),
              Column("d_datetime",JDBCType.TIMESTAMP,true,4,19,0),
              Column("str",JDBCType.VARCHAR,true,2,32,0),
              Column("d_date",JDBCType.DATE,true,3,10,0)
            )))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("parse views metadata") {
    val parser = new MySQLMetadataParser(CONNECTION)
    parser.getTablesMetadata(ObjectType.VIEW, DATABASE, Some(Set(VIEW))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(VIEW,
            Set(),
            Set(Column("primary_key", JDBCType.INTEGER, false, 1, 11, 0),
              Column("b_boolean",JDBCType.BIT,true,7,1,0),
              Column("i_int",JDBCType.INTEGER,true,5,11,0),
              Column("b_bigint",JDBCType.BIGINT,true,6,20,0),
              Column("d_datetime",JDBCType.TIMESTAMP,true,4,19,0),
              Column("str",JDBCType.VARCHAR,true,2,32,0),
              Column("d_date",JDBCType.DATE,true,3,10,0)
            )))
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("skip tables with no records") {
    val parser = new MsSQLMetadataParser(CONNECTION)
    parser.getTablesMetadata(ObjectType.TABLE, DATABASE, Some(Set(NO_RECORDS_TABLE))) match {
      case Success(definitions) =>
        assert(definitions.isEmpty)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  private def createTestTable(): Unit = {
    lazy val query =
      s"""
         |CREATE TABLE $DATABASE.$TABLE (
         |  primary_key INT NOT NULL AUTO_INCREMENT,
         |  str VARCHAR(32),
         |  d_date DATE,
         |  d_datetime DATETIME,
         |  i_int INT,
         |  b_bigint BIGINT,
         |  b_boolean BIT,
         |  PRIMARY KEY (primary_key))
       """.stripMargin
    val stmt = CONNECTION.createStatement()
    stmt.execute(query)
  }

  private def createEmptyTable(): Unit = {
    val query =
      s"""
         |CREATE TABLE $DATABASE.$NO_RECORDS_TABLE (
         |  primary_key INT NOT NULL AUTO_INCREMENT,
         |  str VARCHAR(32),
         |  d_date DATE,
         |  d_datetime DATETIME,
         |  i_int INT,
         |  b_bigint BIGINT,
         |  b_boolean BIT,
         |  PRIMARY KEY (primary_key))
       """.stripMargin
    val stmt = CONNECTION.createStatement()
    stmt.execute(query)
  }

  private def insertTestData(): Unit = {
    lazy val query =
      s"""
         |INSERT INTO $DATABASE.$TABLE
         |  (str, d_date, d_datetime, i_int, b_bigint, b_boolean)
         |VALUES
         |  ('test', CURRENT_DATE, CURRENT_TIMESTAMP, 1, 1, 0)
       """.stripMargin
    val stmt = CONNECTION.createStatement()
    stmt.execute(query)
  }

  private def createTestView(): Unit = {
    val query =
      s"""
         |CREATE VIEW $VIEW AS SELECT * FROM $TABLE
       """.stripMargin
    val stmt = CONNECTION.createStatement()
    stmt.execute(query)
  }
}

