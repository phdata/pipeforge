package io.phdata.jdbc

import io.phdata.jdbc.domain.Configuration
import com.typesafe.config.ConfigFactory


object ParserMain {
  def main(args: Array[String]) = {
    val configuration = parseConfig("application.conf")
    DatabaseMetadataParser.parse(configuration)




  }

  def parseConfig(configName: String) = {
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
