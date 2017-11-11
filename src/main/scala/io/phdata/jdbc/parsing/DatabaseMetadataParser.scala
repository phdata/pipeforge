package io.phdata.jdbc.parsing

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.DatabaseConf
import io.phdata.jdbc.domain.{Column, Table}
import io.phdata.jdbc.util.ExceptionUtil._

import scala.util.{Failure, Success, Try}

trait DatabaseMetadataParser extends LazyLogging {
  def connection: Connection

  def listTablesStatement(schema: String): String

  def getTablesStatement(schema: String, table: String): String

  def getColumnDefinitions(schema: String, table: String): Set[Column]

  def getTablesMetadata(schema: String): Set[Try[Table]] = {
    val tables = listTables(schema)

    tables.map { t =>
      Try(getTableMetadata(schema, t))
        .messageOnFailure(
          s"Error getting metadata for $schema.$t"
        )
    }
  }

  def getTableMetadata(schema: String, table: String): Table = {
    val allColumns = getColumnDefinitions(schema, table)
    val pks = primaryKeys(schema, table, allColumns)
    val columns = allColumns.diff(pks)
    Table(table, pks, columns)
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

  def metadata = connection.getMetaData

  def listTables(schema: String): Set[String] = {
    val stmt: Statement = newStatement
    val query = listTablesStatement(schema)
    logger.debug("Executing query: {}", query)
    results(stmt.executeQuery(query))(_.getString(1)).toSet
  }

  def newStatement = connection.createStatement()

  protected def results[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}

object DatabaseMetadataParser extends LazyLogging {
  def parse(configuration: DatabaseConf): Set[Table] = {
    logger.info("Extracting metadata information: {}", configuration)

    val results = getConnection(configuration) match {
      case Success(connection) =>
        configuration.databaseType.toLowerCase match {
          case "mysql" =>
            new MySQLMetadataParser(connection)
              .getTablesMetadata(configuration.schema)
          case "oracle" =>
            new OracleMetadataParser(connection)
              .getTablesMetadata(configuration.schema)
          case _ =>
            Set(Failure(new Exception(
              s"Metadata parser for database type: " +
                s"${configuration.databaseType} has not been configured")))
        }
      case Failure(e) =>
        logger.error(s"Failed connecting to: $configuration", e)
        throw e
    }

    results.flatMap(x =>
      x match {
        case Success(v) => Some(v)
        case Failure(e) =>
          logger.warn(s"${e.getMessage} ${e.getStackTrace}")
          None
      })
  }

  def getConnection(configuration: DatabaseConf) =
    Try(
      DriverManager.getConnection(configuration.jdbcUrl,
        configuration.username,
        configuration.password))

}
