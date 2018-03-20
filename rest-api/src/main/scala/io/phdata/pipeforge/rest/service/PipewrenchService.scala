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

package io.phdata.pipeforge.rest.service

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.domain.{Environment, Status}
import io.phdata.pipeforge.rest.module.ConfigurationModule
import io.phdata.pipewrench.PipewrenchImpl
import io.phdata.pipewrench.domain.{Configuration, YamlSupport}
import io.phdata.pipeforge.rest.domain.Implicits._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

trait PipewrenchService {

  def getConfiguration(password: String, environment: Environment): Try[Configuration]

  def saveConfiguration(configuration: Configuration): Status

  def saveEnvironment(environment: Environment): Status

  def executePipewrenchMerge(group: String, name: String, template: String): Status

}

class PipewrenchServiceImpl()(implicit executionContext: ExecutionContext)
    extends PipewrenchService
    with ConfigurationModule
    with YamlSupport
    with LazyLogging {

  import sys.process._

  override def getConfiguration(password: String,
                                environment: Environment): Try[Configuration] =
    PipewrenchImpl.buildConfiguration(environment.toDatabaseConfig(password),
                                      environment.metadata,
                                      environment.toPipewrenchEnvironment)

  override def saveConfiguration(configuration: Configuration): Status =
    status(Try {
      createIngestDirIfNotExist(configuration.group, configuration.name)

      configuration.writeYamlFile(tableFilePath(configuration.group, configuration.name))
    })

  override def saveEnvironment(environment: Environment): Status =
    status(Try {
      createIngestDirIfNotExist(environment.group, environment.name)
      environment.toPipewrenchEnvironment.writeYamlFile(envFilePath(environment.group, environment.name))
    })

  override def executePipewrenchMerge(group: String, name: String, template: String): Status =
    status(Try {
      val dir = pipewrenchProjectDir(group, name)
      val templateDir = new File(s"$pipewrenchTemplatesDir/$template").getAbsolutePath
      val projectDir = new File(pipewrenchProjectDir(group, name)).getAbsolutePath
      val cmd = s"$pipewrenchDir/generate-scripts.sh -e env.yml -c tables.yml -t $templateDir -d $projectDir"
      logger.debug(s"CMD: $cmd")
      cmd !!
    })

  private def status(proc: Try[Unit]): Status =
    proc match {
      case Success(_)  => Status("SUCCESS", "Everything is awesome!")
      case Failure(ex) => Status("FAILURE", ex.getMessage)
    }

  private def createIngestDirIfNotExist(group: String, name: String): Unit = {
    val dir = new File(pipewrenchProjectDir(group, name))
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

}
