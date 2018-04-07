package io.phdata.pipewrench

import com.typesafe.config.{ Config, ConfigFactory }

trait AppConfiguration {

  val config: Config = ConfigFactory.load()

  val virtualInstall: Boolean        = config.getBoolean("pipewrench.virtualInstall")
  val pipewrenchGitUrl: String       = config.getString("pipewrench.git.url")
  val installScriptDir: String       = config.getString("pipewrench.directory.install")
  val pipewrenchDir: String          = config.getString("pipewrench.directory.pipewrench")
  val pipewrenchTemplatesDir: String = config.getString("pipewrench.directory.templates")
  val pipewrenchIngestConf: String   = config.getString("pipewrench.directory.ingest")

  val impalaCmd: String = config.getString("impala.cmd")
}
