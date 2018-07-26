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
import net.ceedubs.ficus.Ficus._

/**
 * Trait for parsing application configuration parameters
 */
trait AppConfiguration {

  lazy val config: Config = ConfigFactory.load()

  lazy val virtualInstall: Boolean  = config.as[Boolean]("pipewrench.virtualInstall")
  lazy val installScriptDir: String = config.as[String]("pipewrench.directory.install")

  lazy val pipewrenchGitUrl: String       = config.as[String]("pipewrench.git.url")
  lazy val pipewrenchDir: String          = config.as[String]("pipewrench.directory.pipewrench")
  lazy val pipewrenchTemplatesDir: String = config.as[String]("pipewrench.directory.templates")
  lazy val pipewrenchIngestConf: String   = config.as[String]("pipewrench.directory.ingest")

  lazy val impalaCmd = config.as[String]("impala.cmd")
  lazy val impalaHostOpt = config.as[Option[String]]("impala.hostname")
  lazy val impalaPortOpt = config.as[Option[Int]]("impala.port")

  lazy val hiveMetastoreUrl          = config.as[Option[String]]("hive.metastore.url")
  lazy val hiveMetasotreSchema       = config.as[Option[String]]("hive.metastore.schema")
  lazy val hiveMetastoreUsername     = config.as[Option[String]]("hive.metastore.username")
  lazy val hiveMetastorePassword     = config.as[Option[String]]("hive.metastore.password")
  lazy val hiveMetastoreDatabaseType = config.as[Option[String]]("hive.metastore.databaseType")

}
