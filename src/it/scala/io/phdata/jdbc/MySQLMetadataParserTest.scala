package io.phdata.jdbc

import java.sql.{JDBCType, ResultSet}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, MySQLMetadataParser}
import org.scalatest._
import org.testcontainers.containers.MySQLContainer

import scala.util.{Failure, Success}

class MySQLMetadataParserTest extends FunSuite with BeforeAndAfterAll with LazyLogging {

  private lazy val testDb = new MySQLContainer()
  private lazy val databaseName = "test"
  private lazy val tableName = "it_table"
  private lazy val viewName = "it_view"

  private lazy val dockerConfig = new DatabaseConf(DatabaseType.MYSQL,
    databaseName,
    testDb.getJdbcUrl,
    testDb.getUsername,
    testDb.getPassword,
    ObjectType.TABLE)

  private lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDb.withExposedPorts(MySQLContainer.MYSQL_PORT)
    testDb.start()
    createTestTable()
    insertTestData()
    createTestView()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testDb.stop()
  }

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT table_name FROM information_schema.tables")
    val results = getResults(rs)(x => x.getString(1)).toList
    assertResult(63)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MySQLMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.TABLE, databaseName, None) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(tableName,
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
    parser.getTablesMetadata(ObjectType.VIEW, databaseName, Some(Set(viewName))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(viewName,
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
         |CREATE TABLE $databaseName.$tableName (
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
         |INSERT INTO $databaseName.$tableName
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
         |CREATE VIEW $viewName AS SELECT * FROM $tableName
       """.stripMargin
    val stmt = connection.createStatement()
    stmt.execute(query)
  }
  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}

