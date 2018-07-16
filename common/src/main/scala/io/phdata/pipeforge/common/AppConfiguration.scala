/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.pipeforge.common

import com.typesafe.config.{ Config, ConfigFactory }

/**
 * Trait for parsing application configuration parameters
 */
trait AppConfiguration {

  // Parse application configuration
  lazy val config: Config = ConfigFactory.load()

  // Indicates whether or not to install Pipewrench
  lazy val virtualInstall: Boolean = config.getBoolean("pipewrench.virtualInstall")
  // Git url for Pipewrench
  lazy val pipewrenchGitUrl: String = config.getString("pipewrench.git.url")
  // Directory containing installation scripts
  lazy val installScriptDir: String = config.getString("pipewrench.directory.install")
  // Pipewrench installation directory
  lazy val pipewrenchDir: String = config.getString("pipewrench.directory.pipewrench")
  // Pipewrench template directory
  lazy val pipewrenchTemplatesDir: String = config.getString("pipewrench.directory.templates")
  // Pipewrench ingest config output directory
  lazy val pipewrenchIngestConf: String = config.getString("pipewrench.directory.ingest")
  // Cluster specific impala shell command
  lazy val impalaHost: String = config.getString("impala.hostname")
  lazy val impalaPort: Int    = config.getInt("impala.port")

  lazy val impalaCmd: String = {
    def ssl = if (config.getBoolean("impala.ssl")) "-ssl " else  ""
    def kerberos = if (config.getBoolean("impala.kerberos")) "-k " else ""
    s"impala-shell -i $impalaHost:$impalaPort $ssl $kerberos -f "
  }
}
