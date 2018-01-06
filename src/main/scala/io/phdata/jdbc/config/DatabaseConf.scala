package io.phdata.jdbc.config

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Defines the database-configuration file
  *
  * @param databaseType Source database type can be 'oracle', 'mysql', or 'mssql'
  * @param schema       Schema or database to parse
  * @param jdbcUrl      JDBC connection string url
  * @param username     The username
  * @param password     The password
  * @param objectType   Database objects to parse can be 'table' or 'view'
  * @param tables       White listing of tables to parse
  */
case class DatabaseConf(databaseType: DatabaseType.Value,
                        schema: String,
                        jdbcUrl: String,
                        username: String,
                        password: String,
                        objectType: ObjectType.Value,
                        tables: Option[Set[String]] = None)

/**
  * Parses configuration file into DatabaseConf object
  */
object DatabaseConf {
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.EnumerationReader._

  /**
    * Converts database configuration file into DatabaseConf object
    * @param path Database configuration file path
    * @param password Database user password
    * @return DatabaseConf
    */
  def parse(path: String, password: String) = {
    val file = new File(path)
    val configFactory = ConfigFactory.parseFile(file)

    new DatabaseConf(
      databaseType = configFactory.as[DatabaseType.Value]("database-type"),
      schema = configFactory.as[String]("schema"),
      jdbcUrl = configFactory.as[String]("jdbc-url"),
      username = configFactory.as[String]("username"),
      password = password,
      objectType = configFactory.as[ObjectType.Value]("object-type"),
      tables = configFactory.as[Option[Set[String]]]("tables")
    )
  }
}
