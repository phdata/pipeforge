package io.phdata.jdbc

import java.sql.{JDBCType, ResultSet}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, OracleMetadataParser}
import org.scalatest._
import org.testcontainers.containers.OracleContainer

import scala.util.{Failure, Success}

class OracleMetadataParserTest extends FunSuite with BeforeAndAfterAll with LazyLogging {
  lazy val oracle = new OracleContainer()

  lazy val dockerConfig =
    new DatabaseConf(DatabaseType.ORACLE,
                     "HR",
                     oracle.getJdbcUrl,
                     oracle.getUsername,
                     oracle.getPassword,
                     ObjectType.TABLE)

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
      stmt.executeQuery(
        "SELECT owner, table_name FROM ALL_TABLES where owner = 'HR'")
    val results =
      getResults(rs)(x => x.getString(1) + "." + x.getString(2)).toList
    assertResult(7)(results.length)

  }

  test("parse tables metadata") {
    val parser = new OracleMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.TABLE, "HR", None) match {
      case Success(definitions) =>
        assert(definitions.size == 7)
        val expected =
          Table("REGIONS",
            Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1, 0, -127)),
            Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2, 25, 0)))

        assert(definitions.map(x => x == expected).reduce(_ || _))
      case Failure(ex) =>
        logger.error("Error gathering metadata from source", ex)
    }
  }

  test("parse views metadata") {
    val parser = new OracleMetadataParser(connection)
    parser.getTablesMetadata(ObjectType.VIEW, "HR", None) match {
      case Success(definitions) =>
        val expected = Set(
          Table(
            "EMP_DETAILS_VIEW",
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

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}
