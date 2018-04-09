package io.phdata.pipewrench

import com.typesafe.config.{ Config, ConfigFactory }

/**
 * Trait for parsing application configuration parameters
 */
trait AppConfiguration {

  // Parse application configuration
  val config: Config = ConfigFactory.load()

  // Indicates whether or not to install Pipewrench
  val virtualInstall: Boolean = config.getBoolean("pipewrench.virtualInstall")
  // Git url for Pipewrench
  val pipewrenchGitUrl: String = config.getString("pipewrench.git.url")
  // Directory containing installation scripts
  val installScriptDir: String = config.getString("pipewrench.directory.install")
  // Pipewrench installation directory
  val pipewrenchDir: String = config.getString("pipewrench.directory.pipewrench")
  // Pipewrench template directory
  val pipewrenchTemplatesDir: String = config.getString("pipewrench.directory.templates")
  // Pipewrench ingest config output directory
  val pipewrenchIngestConf: String = config.getString("pipewrench.directory.ingest")
  // Cluster specific impala shell command
  val impalaCmd: String = config.getString("impala.cmd")

}
