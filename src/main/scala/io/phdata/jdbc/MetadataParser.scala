package io.phdata.jdbc

import java.sql.Connection

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.domain.Table

import scala.util.Try

class MetadataParser(connection: Connection) extends DatabaseMetadataParser with LazyLogging {

  val META = connection.getMetaData

  override def getTableDefinitions(schema: String, tables: Seq[String]): Try[Set[Table]] = {
    tables.map {
      table =>
        META.getTables(null, schema, table, "TABLE")
        ???
    }
  }
}
