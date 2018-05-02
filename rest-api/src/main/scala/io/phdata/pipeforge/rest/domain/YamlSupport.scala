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

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._

import scala.io.Source

/**
 * Provides Yaml support
 */
trait YamlSupport extends DefaultYamlProtocol {

  implicit def databaseYamlFormat    = yamlFormat2(Database)
  implicit def environmentYamlFormat = yamlFormat13(Environment.apply)

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
