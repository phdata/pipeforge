package io.phdata.pipeforge.jdbc

import java.sql.{ Connection, DriverManager }

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.Environment
import io.phdata.pipeforge.jdbc.Implicits._

trait SchemaValidator {

  def validateSchema()

}

object SchemaValidator extends SchemaValidator with LazyLogging {

  override def validateSchema(): Unit = {
    val query  = "show tables"
    val stmt   = getConnection().createStatement()
    val tables = stmt.executeQuery(query).toStream.map(_.getString(1)).toList
    logger.info(tables.toString)
  }

  private def getConnection(): Connection = {
    val url = "jdbc:hive2://worker1.valhalla.phdata.io:21050/default;ssl=true;AuthMech=3"
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(url, "tafokken", "Duke1988")
  }
}
