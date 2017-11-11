package io.phdata.jdbc

import java.sql.ResultSet

import io.phdata.jdbc.config.DatabaseConf
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, MySQLMetadataParser, OracleMetadataParser}
import org.scalatest._
import org.testcontainers.containers.MySQLContainer

class MysqlMetadataParserTest extends FunSuite with BeforeAndAfterAll {
  lazy val testDb = new MySQLContainer()

  lazy val dockerConfig = new DatabaseConf("mysql",
    "HR",
    testDb.getJdbcUrl,
    testDb.getUsername,
    testDb.getPassword)

  lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDb.withExposedPorts(1521)
    testDb.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testDb.stop()
  }

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet =
      stmt.executeQuery("SELECT table_name FROM information_schema.tables")
    val results =
      getResults(rs)(x => x.getString(1)).toList
    assertResult(61)(results.length)
  }

  test("parse tables metadata") {
    val parser = new MySQLMetadataParser(connection)
    val definitions = parser.getTablesMetadata("HR")
    definitions.foreach(println)
    assert(definitions.size == 0)
  }

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}

