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

package io.phdata.pipeforge.rest.module

import com.typesafe.config.{ Config, ConfigFactory }

trait ConfigurationModule {

  val configuration: Config = ConfigFactory.load()

  val virtualInstall         = configuration.getBoolean("pipewrench.virtualInstall")
  val pipewrenchGitUrl       = configuration.getString("pipewrench.git.url")
  val installScript          = configuration.getString("pipewrench.scripts.install")
  val baseDir                = configuration.getString("pipewrench.directory.base")
  val pipewrenchDir          = configuration.getString("pipewrench.directory.pipewrench")
  val pipewrenchTemplatesDir = configuration.getString("pipewrench.directory.templates")
  val pipewrenchIngestConf   = configuration.getString("pipewrench.directory.ingest")

  def pipewrenchProjectDir(group: String, name: String) = s"$pipewrenchIngestConf/$group/$name"

  def tableFilePath(group: String, name: String) =
    s"${pipewrenchProjectDir(group, name)}/tables.yml"
  def envFilePath(group: String, name: String) = s"${pipewrenchProjectDir(group, name)}/env.yml"

}
