package io.phdata.jdbc.parsing

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.jdbc.domain.{Column, Table}

import scala.util.{Failure, Success, Try}

trait DatabaseMetadataParser extends LazyLogging {
  def connection: Connection

  def listTablesStatement(schema: String): String

  def singleRecordQuery(schema: String, table: String): String

  def listViewsStatement(schema: String): String

  def getColumnDefinitions(schema: String, table: String): Set[Column]

  def getTablesMetadata(objectType: ObjectType.Value,
                        schema: String,
                        tableWhiteList: Option[Set[String]]): Try[Set[Table]] = {
    // If a white listing of tables is provided then only parse those tables
    val tables = tableWhiteList match {
      // TODO: Add check to see if whitelisted tables are in the source database before parsing.  Throw ex if a table is not found
      case Some(t) => t
      case None => listTables(objectType, schema)
    }

    Try(
      tables.map { t =>
        getTableMetadata(schema, t)
      }
    )
  }

  def getTableMetadata(schema: String, table: String): Table = {
    val allColumns = getColumnDefinitions(schema, table)
    val pks = primaryKeys(schema, table, allColumns)
    val columns = allColumns.diff(pks)
    Table(table, pks, columns)
  }

  def primaryKeys(schema: String,
                            table: String,
                            columns: Set[Column]): Set[Column] = {
    logger.trace("Getting primary keys for schema: {}, table: {}",
                 schema,
                 table)
    val rs: ResultSet = metadata.getPrimaryKeys(schema, schema, table)
    val pks = results(rs) { record =>
      record.getString("COLUMN_NAME") -> record.getInt("KEY_SEQ")
    }.toMap

    mapPrimaryKeyToColumn(pks, columns)
  }

  def mapMetaDataToColumn(metaData: ResultSetMetaData, rsMetadata: ResultSetMetaData) = {
    def asBoolean(i: Int) = if (i == 0) false else true

    (1 to metaData.getColumnCount).map { i =>
      Column(
        metaData.getColumnName(i),
        JDBCType.valueOf(rsMetadata.getColumnType(i)),
        asBoolean(metaData.isNullable(i)),
        i,
        metaData.getPrecision(i),
        metaData.getScale(i)
      )
    }.toSet
  }

  def mapPrimaryKeyToColumn(primaryKeys: Map[String, Int], columns: Set[Column]) = {
    primaryKeys.flatMap {
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

  def listTables(objectType: ObjectType.Value, schema: String): Set[String] = {
    val stmt: Statement = newStatement
    val query =
      if (objectType == ObjectType.TABLE) listTablesStatement(schema)
      else listViewsStatement(schema)
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
  def parse(configuration: DatabaseConf): Try[Set[Table]] = {
    logger.info("Extracting metadata information: {}", configuration)

    getConnection(configuration) match {
      case Success(connection) =>
        configuration.databaseType match {
          case DatabaseType.MYSQL =>
            new MySQLMetadataParser(connection)
              .getTablesMetadata(configuration.objectType, configuration.schema, configuration.tables)
          case DatabaseType.ORACLE =>
            new OracleMetadataParser(connection)
              .getTablesMetadata(configuration.objectType, configuration.schema, configuration.tables)
          case DatabaseType.MSSQL =>
            new MsSQLMetadataParser(connection)
              .getTablesMetadata(configuration.objectType, configuration.schema, configuration.tables)
          case _ =>
              Failure(
                new Exception(s"Metadata parser for database type: " +
                  s"${configuration.databaseType} has not been configured"))
        }
      case Failure(e) =>
        logger.error(s"Failed connecting to: $configuration", e)
        throw e
    }
  }

  def getConnection(configuration: DatabaseConf) = {
    val f = Try(
      DriverManager.getConnection(configuration.jdbcUrl,
        configuration.username,
        configuration.password))

    f match {
      case Success(con) => con
      case Failure(ex) => logger.error("Error", ex)
    }

    f
  }

}
