package io.phdata.jdbc

import java.sql.{JDBCType, ResultSet}

import com.whisk.docker.{DockerContainer, DockerReadyChecker}
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, MySQLMetadataParser}

import scala.util.{Failure, Success}

class MySQLMetadataParserTest extends DockerTestRunner {

  private lazy val ROOT_PASS = "root"
  private lazy val DATABASE = "it_test"
  private lazy val USER = "it_user"
  private lazy val PASSWORD = "it_test"
  private lazy val TABLE = "it_table"
  private lazy val VIEW = "it_view"

  override val image = "mysql"

  override val advertisedPort = 3306

  override val exposedPort = 3306

  override val container = DockerContainer(image)
    .withPorts((advertisedPort, Some(exposedPort)))
    .withEnv(
      s"MYSQL_ROOT_PASSWORD=$ROOT_PASS",
      s"MYSQL_DATABASE=$DATABASE",
      s"MYSQL_USER=$USER",
      s"MYSQL_PASSWORD=$PASSWORD")
    .withReadyChecker(DockerReadyChecker.LogLineContains("mysqld: ready for connections"))

  private lazy val URL = s"jdbc:mysql://${container.hostname.getOrElse("localhost")}:$exposedPort/$DATABASE"
  private lazy val DRIVER = "com.mysql.jdbc.Driver"

  override def beforeAll(): Unit = {
    super.beforeAll()
    startAllOrFail()
    Thread.sleep(5000)
    createTestTable()
    insertTestData()
    createTestView()
  }

  override def afterAll(): Unit = {
    stopAllQuietly()
    super.afterAll()
  }

  private lazy val dockerConfig = new DatabaseConf(DatabaseType.MYSQL,
    DATABASE,
    URL,
    USER,
    PASSWORD,
    ObjectType.TABLE)

  private lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT table_name FROM information_schema.tables")
    val results = getResults(rs)(x => x.getString(1)).toList
    assertResult(63)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MySQLMetadataParser(connection)
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
    val parser = new MySQLMetadataParser(connection)
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
    val stmt = connection.createStatement()
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
    val stmt = connection.createStatement()
    stmt.execute(query)
  }

  private def createTestView(): Unit = {
    val query =
      s"""
         |CREATE VIEW $VIEW AS SELECT * FROM $TABLE
       """.stripMargin
    val stmt = connection.createStatement()
    stmt.execute(query)
  }
}

