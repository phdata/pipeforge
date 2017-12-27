package io.phdata.jdbc.config

import com.typesafe.config.ConfigFactory

/**
  * Configuration object an parser for pipewrench specific configuration
  *
  * @param defaultSourceConfPath Path to a yaml file containing key-value pairs
  *                              that will be included in the header of a
  *                              `tables.yml` configuration. This configuration
  *                              would include things like connection string,
  *                              username, and a parameterized password.
  * @param defaultTablesConfPath Path to a yaml file containing key-value pairs
  *                              that will be included in each table element
  *                              in the generated config. This file would
  *                              include things like metadata tags.
  * @param outFile               Path where generated configuration will be written
  */
case class PipewrenchConf(defaultSourceConfPath: String,
                          defaultTablesConfPath: String,
                          outFile: String)

object PipewrenchConf {
  import net.ceedubs.ficus.Ficus._

  def parse(configName: String) = {
    val configFactory = ConfigFactory.load(configName)

    new PipewrenchConf(
      defaultSourceConfPath =
        configFactory.as[String]("default-source-conf-path"),
      defaultTablesConfPath =
        configFactory.as[String]("default-tables-conf-path"),
      outFile = configFactory.as[String]("out-file")
    )
  }
}
