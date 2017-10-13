package io.phdata.jdbc

import java.sql.{Connection, JDBCType, ResultSet, Statement}

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.{Column, Table}

import scala.util.{Failure, Success, Try}

class MySQLMetadataParser(connection: Connection) extends DatabaseMetadataParser with LazyLogging {

  lazy val META = connection.getMetaData

  override def getTableDefinitions(schema: String, tables: Seq[String]): Try[Set[Table]] = {
    try {
      val tables = listTables.map {
        table =>
          val allColumns = getColumnDefinitions(table)
          val pks = primaryKeys(schema, table, allColumns)
          val columns = allColumns.diff(pks)
          Table(table, pks, columns)
      }
      Success(tables)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        Failure(e)
    } finally {
      connection.close()
    }
  }

  private def listTables: Set[String] = {
    val stmt: Statement = connection.createStatement()
    val query = "SHOW TABLES"
    logger.trace("Executing query: {}", query)
    results(stmt.executeQuery(query))(_.getString(1)).toSet
  }

  private def primaryKeys(schema: String, table: String, columns: Set[Column]): Set[Column] = {
    logger.trace("Getting primary keys for schema: {}, table: {}", schema, table)
    val rs: ResultSet = META.getPrimaryKeys(schema, schema, table)
    val pks = results(rs) {
      record =>
        record.getString("COLUMN_NAME") -> record.getInt("KEY_SEQ")
    }.toMap

    pks.flatMap {
      case (key, index) =>
        columns.find(_.name == key) match {
          case Some(column) => Some(Column(column.name, column.dataType, column.nullable, column.index))
          case None => None
        }
    }.toSet
  }

  private def getColumnDefinitions(table: String): Set[Column] = {
    def asBoolean(i: Int) = if (i == 0) false else true
    val stmt = connection.createStatement()
    val query = s"SELECT * FROM $table LIMIT 1"
    logger.trace("Executing query: {}", query)
    val metaData = results(stmt.executeQuery(query))(_.getMetaData).toList.head

    (1 to metaData.getColumnCount).map {
      i =>
        Column(
          metaData.getColumnName(i),
          JDBCType.valueOf(metaData.getColumnType(i)),
          asBoolean(metaData.isNullable(i)), i)
    }.toSet
  }
}
