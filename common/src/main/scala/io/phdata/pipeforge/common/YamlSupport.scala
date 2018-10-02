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

import java.io.FileWriter

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import io.phdata.pipeforge.common.pipewrench.{ Column, Configuration, Kudu, Table, Environment => PipewrenchEnvironment }

import scala.io.Source

trait YamlSupport extends DefaultYamlProtocol with LazyLogging {

  implicit def databaseYamlFormat    = yamlFormat2(Database)
  implicit def environmentYamlFormat = yamlFormat15(Environment.apply)

  implicit def pipewrenchEnvironmentFormat   = yamlFormat10(PipewrenchEnvironment)
  implicit def pipewrenchColumnFormat        = yamlFormat5(Column)
  implicit def pipewrenchKuduFormat          = yamlFormat2(Kudu)
  implicit def pipewrenchTableFormat         = yamlFormat11(Table)
  implicit def pipewrenchConfigurationFormat = yamlFormat14(Configuration)

  /**
   * Parses input file into Environment object
   * @param path
   * @return
   */
  def parseEnvironmentFile(path: String): Environment =
    readFile(path).parseYaml.convertTo[Environment]

  def parseConfigurationFile(path: String): Configuration =
    readFile(path).parseYaml.convertTo[Configuration]

  private def readFile(path: String): String = Source.fromFile(path).getLines.mkString("\n")

  /**
   * Implicit class for writing yaml files
   * @param environment
   */
  implicit class WriteEnvironmentYamlFile(environment: PipewrenchEnvironment) {
    def writeYamlFile(path: String): Unit = writeFile(environment.toYaml, path)
  }

  /**
   * Implicit class for writing yaml files
   * @param configuration
   */
  implicit class WriteConfigurationYamlFile(configuration: Configuration) {
    def writeYamlFile(path: String): Unit = writeFile(configuration.toYaml, path)
  }

  /**
   * Writes a yaml value to a file
   * @param yaml
   * @param path
   */
  private def writeFile(yaml: YamlValue, path: String): Unit = {
    val fw = new FileWriter(path)
    logger.debug(s"Writing file: $path")
    fw.write(yaml.prettyPrint)
    fw.close()
  }
}
