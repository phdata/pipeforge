package io.phdata.jdbc

import java.sql.{JDBCType, ResultSet}

import com.whisk.docker.DockerContainer
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, MsSQLMetadataParser}

import scala.util.{Failure, Success}

class MsSQLMetadataParserTest extends DockerTestRunner {

  import scala.concurrent.duration._

  private lazy val PASSWORD = "!IntegrationTests"
  private lazy val DATABASE = "master"
  private lazy val USER = "SA"
  private lazy val TABLE = "it_table"
  private lazy val VIEW = "it_view"

  override val image = "microsoft/mssql-server-linux:latest"

  override val advertisedPort = 1433

  override val exposedPort = 1433

  override val container = DockerContainer(image)
    .withPorts((advertisedPort, Some(exposedPort)))
    .withEnv("ACCEPT_EULA=Y", s"SA_PASSWORD=$PASSWORD")

  private lazy val URL = s"jdbc:sqlserver://${container.hostname.getOrElse("localhost")}:$exposedPort;database=$DATABASE"
  private lazy val DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  private lazy val dockerConfig = new DatabaseConf(DatabaseType.MSSQL,
    DATABASE,
    URL,
    USER,
    PASSWORD,
    ObjectType.TABLE
  )

   private lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.withReadyChecker(
      new DatabaseReadyChecker(
        DRIVER,
        URL,
        USER,
        PASSWORD,
        DATABASE).looped(30, 5.seconds)
    )
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

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT * FROM sys.tables")
    val results = getResults(rs)(x => x.getString(1)).toList
    assertResult(6)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MsSQLMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.TABLE, DATABASE, Some(Set(TABLE))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(TABLE,
          Set(
            Column("ID",JDBCType.INTEGER,false,1,10,0),
            Column("LastName",JDBCType.VARCHAR,false,2,255,0)),
          Set(
            Column("FirstName",JDBCType.VARCHAR,true,3,255,0),
            Column("Age",JDBCType.INTEGER,true,4,10,0)
          )))
        assertResult(expected)(definitions)

      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("parse views metadata") {
    val parser = new MsSQLMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.VIEW, DATABASE, Some(Set(VIEW))) match {
      case Success(definitions) =>
        assert(definitions.size == 1)
        val expected = Set(
          Table(VIEW,
            Set(),
            Set(
              Column("ID",JDBCType.INTEGER,false,1,10,0),
              Column("LastName",JDBCType.VARCHAR,false,2,255,0),
              Column("FirstName",JDBCType.VARCHAR,true,3,255,0),
              Column("Age",JDBCType.INTEGER,true,4,10,0)))
        )
        assertResult(expected)(definitions)
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  private def createTestDatabase(): Unit = {
    val query =
      s"""
         |CREATE DATABASE $DATABASE
       """.stripMargin
    val stmt = connection.createStatement()
    stmt.execute(query)
  }

  private def createTestTable(): Unit = {
    val query =
      s"""
         |CREATE TABLE $TABLE (
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
         |INSERT INTO $TABLE
         |  (ID, LastName, FirstName, Age)
         |VALUES
         |  (1, 'developer', 'phdata', 1)
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
