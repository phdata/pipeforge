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

  val pipewrenchDir         = configuration.getString("pipewrench.directory.output")
  val pipewrenchTemplateDir = configuration.getString("pipewrench.directory.template")

  val pipewrenchConfigDir = if (pipewrenchDir.startsWith("/")) {
    pipewrenchDir
  } else {
    s"${configuration.getString("user.home")}/$pipewrenchDir"
  }

  def pipewrenchProjectDir(group: String, name: String) = s"$pipewrenchConfigDir/$group/$name"

  def tableFilePath(group: String, name: String) = s"${pipewrenchProjectDir(group, name)}/tables.yml"
  def envFilePath(group: String, name: String) = s"${pipewrenchProjectDir(group, name)}/env.yml"

}
