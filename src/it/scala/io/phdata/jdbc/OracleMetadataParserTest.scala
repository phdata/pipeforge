package io.phdata.jdbc

import java.sql.{Connection, JDBCType, ResultSet}

import io.phdata.jdbc.config.DatabaseConf
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, OracleMetadataParser}
import org.scalatest._
import org.testcontainers.containers.OracleContainer

class OracleMetadataParserTest extends FunSuite with BeforeAndAfterAll {
  lazy val oracle = new OracleContainer()

  lazy val dockerConfig = new DatabaseConf("oracle",
    "HR",
    oracle.getJdbcUrl,
    oracle.getUsername,
    oracle.getPassword)

  lazy val connection = DatabaseMetadataParser.getConnection(dockerConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    oracle.withExposedPorts(1521)
    oracle.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    oracle.stop()
  }

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet =
      stmt.executeQuery("SELECT owner, table_name FROM ALL_TABLES where owner = 'HR'")
    val results =
      getResults(rs)(x => x.getString(1) + "." + x.getString(2)).toList
    assertResult(7)(results.length)
  }

  test("parse tables metadata") {
    val parser = new OracleMetadataParser(connection)
    val definitions = parser.getTablesMetadata("HR")
    definitions.foreach(println)
    assert(definitions.size == 7)
  }

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}
