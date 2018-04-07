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

package io.phdata.pipeforge.rest.domain

import io.phdata.pipeforge.jdbc.config.{ DatabaseConf, DatabaseType, ObjectType }
import io.phdata.pipewrench.domain.{ Environment => PipewrenchEnvironment }

case class Environment(name: String,
                       group: String,
                       databaseType: String,
                       schema: String,
                       jdbcUrl: String,
                       username: String,
                       objectType: String,
                       metadata: Map[String, String],
                       hdfsPath: String,
                       hadoopUser: String,
                       passwordFile: String,
                       destinationDatabase: String,
                       tables: Option[List[String]] = None)

object Environment {
  implicit class EnvironmentPipewrench(environment: Environment) {
    def toPipewrenchEnvironment: PipewrenchEnvironment =
      PipewrenchEnvironment(
        name = environment.name,
        group = environment.group,
        connection_string = environment.jdbcUrl,
        hdfs_basedir = environment.hdfsPath,
        hadoop_user = environment.hadoopUser,
        password_file = environment.passwordFile,
        destination_database = environment.destinationDatabase
      )
  }

  implicit class EnvironmentDatabaseConfig(environment: Environment) {
    def toDatabaseConfig(password: String): DatabaseConf =
      DatabaseConf(
        databaseType = DatabaseType.withName(environment.databaseType),
        schema = environment.schema,
        jdbcUrl = environment.jdbcUrl,
        username = environment.username,
        password = password,
        objectType = ObjectType.withName(environment.objectType),
        tables = environment.tables
      )
  }
}
