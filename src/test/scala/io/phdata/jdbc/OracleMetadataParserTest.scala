package io.phdata.jdbc
import java.sql.{JDBCType, ResultSet, SQLType}

import io.phdata.jdbc.config.Configuration
import oracle.sql
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.parsing.{DatabaseMetadataParser, OracleMetadataParser}
import org.scalatest._

class OracleMetadataParserTest extends FunSuite {
  val config = Configuration.parse("application.conf")

  val connection = DatabaseMetadataParser.getConnection(config).get

  test("run query against database") {
    val stmt = connection.createStatement()
    val rs: ResultSet = stmt.executeQuery(
      "SELECT owner, table_name FROM ALL_TABLES where owner = 'HR'")
    val results =
      getResults(rs)(x => x.getString(1) + "." + x.getString(2)).toList
    results.foreach(println)
    assert(results.length == 7)
  }

  test("parse tables metadata") {
    val parser = new OracleMetadataParser(connection)
    val definitions: Set[Table] = parser.getTablesMetadata("HR").get
    definitions.foreach(println)
    assert(definitions.size == 7)
  }

  test("parse single table metadata") {
    val parser = new OracleMetadataParser(connection)

    val result = parser.getTableMetadata("HR", "REGIONS")
    val expected = Table("REGIONS",
      Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1, 0, -127)),
      Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2, 25, 0)))

    assert(expected == result)
  }

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()
      def next() = f(resultSet)
    }
  }
}
