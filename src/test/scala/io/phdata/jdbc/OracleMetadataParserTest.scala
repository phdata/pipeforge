package io.phdata.jdbc
import java.sql.{JDBCType, ResultSet, SQLType}

import io.phdata.jdbc.domain.{Column, Table}
import org.scalatest._

class OracleMetadataParserTest extends FunSuite {
  val config = ParserMain.parseConfig("application.conf")

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

  test("parse metadata") {
    val parser = new OracleMetadataParser(connection)
    val definitions: Set[Table] = parser.getTableDefinitions("HR").get
    assert(definitions.size == 7)

    val regionTable = definitions.filter(_.name == "REGIONS").head
    val expected = Table("REGIONS",
                         Set(Column("REGION_ID", JDBCType.NUMERIC, false, 1)),
                         Set(Column("REGION_NAME", JDBCType.VARCHAR, true, 2)))

    assert(expected == regionTable)
  }

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()
      def next() = f(resultSet)
    }
  }
}
