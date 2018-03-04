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

package io.phdata.pipeforge.config

import io.phdata.pipeforge.jdbc.config.{ DatabaseConf, DatabaseType, ObjectType }
import net.jcazevedo.moultingyaml.DefaultYamlProtocol

import net.jcazevedo.moultingyaml._

import scala.io.Source

trait YamlProtocol extends DefaultYamlProtocol {

  implicit def environmentFormat = yamlFormat7(Environment)

  /**
   * Converts an Environment object to DatabaseConf object
   * @param environment
   * @param password
   * @return
   */
  def getDatabaseConf(environment: Environment, password: String): DatabaseConf = {
    val tables = environment.tables match {
      case Some(t) => Some(t.toSet)
      case None    => None
    }

    new DatabaseConf(
      databaseType = DatabaseType.withName(environment.databaseType),
      schema = environment.schema,
      jdbcUrl = environment.jdbcUrl,
      username = environment.username,
      password = password,
      objectType = ObjectType.withName(environment.objectType),
      tables = tables
    )
  }

  /**
   * Parses input file into Environment object
   * @param path
   * @return
   */
  def parseFile(path: String): Environment = {
    val file = Source.fromFile(path).getLines.mkString("\n")
    file.parseYaml.convertTo[Environment]
  }
}
