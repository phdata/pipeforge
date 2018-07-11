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
