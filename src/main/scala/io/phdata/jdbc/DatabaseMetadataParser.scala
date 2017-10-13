package io.phdata.jdbc

import java.sql.{DriverManager, ResultSet}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.{Configuration, Table}

import scala.util.{Failure, Success, Try}

trait DatabaseMetadataParser {
  def getTableDefinitions(schema: String, tables: Seq[String] = Seq()): Try[Set[Table]]

  def results[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()
      def next() = f(resultSet)
    }
  }
}

object DatabaseMetadataParser extends LazyLogging {
  def parse(configuration: Configuration): Try[Set[Table]] = {
    logger.info("Extracting metadata information: {}", configuration)

    getConnection(configuration) match {
      case Success(connection) =>
        configuration.databaseType.toLowerCase match {
          case "mysql" => new MySQLMetadataParser(connection).getTableDefinitions(configuration.schema)
          case _ =>
            Failure(new Exception(s"Metadata parser for database type: ${configuration.databaseType} has not been configured"))
        }
      case Failure(e) =>
        logger.error(s"Failed connecting to: $configuration", e)
        Failure(e)
    }
  }

  private def getConnection(configuration: Configuration) =
    Try(DriverManager.getConnection(configuration.jdbcUrl, configuration.username, configuration.password))

}
