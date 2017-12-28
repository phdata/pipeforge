package io.phdata.jdbc

import java.sql.ResultSet

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, MsSQLMetadataParser}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest._
import org.testcontainers.containers.MSSQLServerContainer

import scala.util.{Failure, Success}

class MsSQLMetadataParserTest extends FunSuite with BeforeAndAfterAll with LazyLogging {

  lazy val testDb = new MSSQLServerContainer()

  private lazy val databaseName = "master"
  private lazy val tableName = "it_test"

  lazy val dockerConfig = new DatabaseConf(DatabaseType.MSSQL,
    "master",
    testDb.getJdbcUrl + ";database=master",
    testDb.getUsername,
    testDb.getPassword,
    ObjectType.TABLE
  )

   lazy val sa_connection = DatabaseMetadataParser.getConnection(dockerConfig).get
   lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig.copy(jdbcUrl = s"${dockerConfig.jdbcUrl};database=$databaseName")).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDb.withExposedPorts(MSSQLServerContainer.MS_SQL_SERVER_PORT)
    testDb.start()
    Thread.sleep(10000)
    createTestTable()
    insertTestData()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testDb.stop()
  }

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT * FROM sys.tables")
    val results = getResults(rs)(x => x.getString(1)).toList
    assertResult(6)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MsSQLMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.TABLE, databaseName, Some(Set(tableName))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  private def createTestDatabase(): Unit = {
    val query =
      s"""
         |CREATE DATABASE $databaseName
       """.stripMargin
    val stmt = sa_connection.createStatement()
    stmt.execute(query)
  }

  private def createTestTable(): Unit = {
    val query =
      s"""
         |CREATE TABLE $tableName (
         |  ID int NOT NULL,
         |  LastName varchar(255) NOT NULL,
         |  FirstName varchar(255),
         |  Age int,
         |  CONSTRAINT PK_Person PRIMARY KEY (ID,LastName)
         |)
       """.stripMargin
    val stmt = connection.createStatement()
    stmt.execute(query)
  }

  private def insertTestData(): Unit = {
    val query =
      s"""
         |INSERT INTO $tableName
         |  (ID, LastName, FirstName, Age)
         |VALUES
         |  (1, 'developer', 'phdata', 1)
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
