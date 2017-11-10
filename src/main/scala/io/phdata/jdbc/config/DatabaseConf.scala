package io.phdata.jdbc.config

import com.typesafe.config.ConfigFactory

/**
  *
  * @param databaseType Can be 'oracle' or 'mysql'
  * @param schema Schema or database configuration is to be generated for
  * @param jdbcUrl JDBC connection string url
  * @param username The username
  * @param password The password
  */
case class DatabaseConf(databaseType: String,
                        schema: String,
                        jdbcUrl: String,
                        username: String,
                        password: String)


object DatabaseConf {
  def parse(configName: String) = {
    val configFactory = ConfigFactory.load(configName)

    new DatabaseConf(
      databaseType = configFactory.getString("database-type"),
      schema = configFactory.getString("schema"),
      jdbcUrl = configFactory.getString("jdbc-url"),
      username = configFactory.getString("username"),
      password = configFactory.getString("password")
    )
  }
}
