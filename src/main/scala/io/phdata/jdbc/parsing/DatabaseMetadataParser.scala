package io.phdata.jdbc.parsing

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.DatabaseConf
import io.phdata.jdbc.domain.{Column, Table}
import oracle.jdbc.driver.OracleConnection

import scala.util.{Failure, Success, Try}

trait DatabaseMetadataParser extends LazyLogging {

  def connection: Connection

  def listTablesStatement(schema: String): String

  def metadata = connection.getMetaData

  def newStatement = connection.createStatement()

  def getTablesStatement(schema: String, table: String): String

  def getTablesMetadata(schema: String): Try[Set[Table]] = {
    try {
      val tables = listTables(schema).map { table =>
        getTableMetadata(schema, table)
      }
      Success(tables)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        Failure(e)
    }
  }

  def getTableMetadata(schema: String, table: String) = {
    val allColumns = getColumnDefinitions(schema, table)
    val pks = primaryKeys(schema, table, allColumns)
    val columns = allColumns.diff(pks)
    Table(table, pks, columns)
  }

  def listTables(schema: String): Set[String] = {
    val stmt: Statement = newStatement
    val query = listTablesStatement(schema)
    logger.debug("Executing query: {}", query)
    results(stmt.executeQuery(query))(_.getString(1)).toSet
  }

  protected def getColumnDefinitions(schema: String,
                                     table: String): Set[Column] = {
    def asBoolean(i: Int) = if (i == 0) false else true

    val map = connection.asInstanceOf[OracleConnection].getTypeMap

    val stmt: Statement = newStatement
    val query = getTablesStatement(schema, table)
    logger.debug("Executing query: {}", query)
    val metaData: ResultSetMetaData =
      results(stmt.executeQuery(query))(_.getMetaData).toList.head // _.getOracleObject
    val oracleM = metaData.asInstanceOf[oracle.jdbc.OracleResultSetMetaData]
    (1 to metaData.getColumnCount).map { i =>
      Column(
        metaData.getColumnName(i),
        JDBCType.valueOf(oracleM.getColumnType(i)),
        asBoolean(metaData.isNullable(i)),
        i,
        metaData.getPrecision(i), // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSetMetaData.html
        metaData.getScale(i)
      )
    }.toSet
  }

  protected def results[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }

  protected def primaryKeys(schema: String,
                            table: String,
                            columns: Set[Column]): Set[Column] = {
    logger.trace("Getting primary keys for schema: {}, table: {}",
      schema,
      table)
    val rs: ResultSet = metadata.getPrimaryKeys(schema, schema, table)
    val pks = results(rs) { record =>
      record.getString("COLUMN_NAME") -> record.getInt("KEY_SEQ")
    }.toMap

    pks.flatMap {
      case (key, index) =>
        columns.find(_.name == key) match {
          case Some(column) =>
            Some(
              Column(column.name,
                column.dataType,
                column.nullable,
                column.index,
                column.precision,
                column.scale))
          case None => None
        }
    }.toSet
  }
}

object DatabaseMetadataParser extends LazyLogging {
  def parse(configuration: DatabaseConf): Try[Set[Table]] = {
    logger.info("Extracting metadata information: {}", configuration)

    getConnection(configuration) match {
      case Success(connection) =>
        configuration.databaseType.toLowerCase match {
          case "mysql" =>
            new MySQLMetadataParser(connection)
              .getTablesMetadata(configuration.schema)
          case "oracle" =>
            new OracleMetadataParser(connection)
              .getTablesMetadata(configuration.schema)
          case _ =>
            Failure(new Exception(
              s"Metadata parser for database type: ${configuration.databaseType} has not been configured"))
        }
      case Failure(e) =>
        logger.error(s"Failed connecting to: $configuration", e)
        Failure(e)
    }
  }

  def getConnection(configuration: DatabaseConf) =
    Try(
      DriverManager.getConnection(configuration.jdbcUrl,
        configuration.username,
        configuration.password))

}
