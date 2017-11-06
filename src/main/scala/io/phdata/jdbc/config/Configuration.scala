package io.phdata.jdbc.config

import com.typesafe.config.ConfigFactory

case class Configuration(databaseType: String,
                         schema: String,
                         jdbcUrl: String,
                         username: String,
                         password: String)


object Configuration {
  def parse(configName: String) = {
    val configFactory = ConfigFactory.load(configName)

    new Configuration(
      databaseType = configFactory.getString("database-type"),
      schema = configFactory.getString("schema"),
      jdbcUrl = configFactory.getString("jdbc-url"),
      username = configFactory.getString("username"),
      password = configFactory.getString("password")
    )
  }
}
