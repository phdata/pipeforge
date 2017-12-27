package io.phdata.jdbc.config

import com.typesafe.config.ConfigFactory

/**
  *
  * @param databaseType Can be 'oracle' or 'mysql'
  * @param schema       Schema or database configuration is to be generated for
  * @param jdbcUrl      JDBC connection string url
  * @param username     The username
  * @param password     The password
  */
case class DatabaseConf(databaseType: String,
                        schema: String,
                        jdbcUrl: String,
                        username: String,
                        password: String,
                        objectType: ObjectType.Value,
                        tables: Option[Set[String]] = None)

object DatabaseConf {
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.EnumerationReader._

  def parse(configName: String) = {
    val configFactory = ConfigFactory.load(configName)

    new DatabaseConf(
      databaseType = configFactory.as[String]("database-type"),
      schema = configFactory.as[String]("schema"),
      jdbcUrl = configFactory.as[String]("jdbc-url"),
      username = configFactory.as[String]("username"),
      password = configFactory.as[String]("password"),
      objectType = configFactory.as[ObjectType.Value]("object-type"),
      tables = configFactory.as[Option[Set[String]]]("tables")
    )
  }
}
