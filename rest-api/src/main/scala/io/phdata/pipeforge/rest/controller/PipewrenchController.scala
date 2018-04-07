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

package io.phdata.pipeforge.rest.controller

import akka.http.scaladsl.model.{ ContentTypes, StatusCodes }
import akka.http.scaladsl.server.Directives._
import io.phdata.pipeforge.rest.domain.Environment
import io.phdata.pipewrench.Pipewrench
import io.phdata.pipewrench.domain.Configuration
import io.phdata.pipewrench.domain.{ YamlSupport => PipewrenchYamlSupport }
import net.jcazevedo.moultingyaml._

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class PipewrenchController(pipewrenchService: Pipewrench)(
    implicit executionContext: ExecutionContext)
    extends Handlers
    with PipewrenchYamlSupport {

  val basePath = "pipewrench"

  val route =
    handleExceptions(exceptionHandler) {
      extractRequest { request =>
        path(basePath) {
          get {
            complete(s"Pipewrench Rest Api")
          }
        } ~
        path(basePath / "merge") {
          post {
            parameter('template) { template =>
              request.entity.contentType match {
                case ContentTypes.`application/json` =>
                  entity(as[Configuration]) { configuration =>
                    merge(template, configuration)
                    complete(StatusCodes.Created)
                  }
                case _ =>
                  entity(as[String]) { yamlStr =>
                    val configuration = yamlStr.parseYaml.convertTo[Configuration]
                    merge(template, configuration)
                    complete(StatusCodes.Created)
                  }
              }
            }
          }
        } ~
        path(basePath / "configuration") {
          put {
            decodePassword(request) match {
              case Success(password) =>
                request.entity.contentType match {
                  case ContentTypes.`application/json` =>
                    entity(as[Environment]) { environment =>
                      buildConfiguration(password, environment) match {
                        case Success(configuration) => complete(configuration)
                        case Failure(ex)            => ex
                      }
                    }
                  case _ =>
                    entity(as[String]) { yamlStr =>
                      val environment = yamlStr.parseYaml.convertTo[Environment]
                      buildConfiguration(password, environment) match {
                        case Success(configuration) =>
                          val yaml = configuration.toYaml.prettyPrint
                          complete(yaml)
                        case Failure(ex) => ex
                      }
                    }
                }
              case Failure(ex) => ex
            }
          }
        } ~
        path(basePath / "environment") {
          post {
            request.entity.contentType match {
              case ContentTypes.`application/json` =>
                entity(as[Environment]) { environment =>
                  saveEnvironment(environment)
                  complete(StatusCodes.Created)
                }
              case _ =>
                entity(as[String]) { yamlStr =>
                  val environment = yamlStr.parseYaml.convertTo[Environment]
                  saveEnvironment(environment)
                  complete(StatusCodes.Created)
                }
            }
          }
        }
      }
    }

  def merge(template: String, configuration: Configuration) = {
    pipewrenchService.saveConfiguration(configuration)
    pipewrenchService.executePipewrenchMergeApi(template, configuration)
  }

  def buildConfiguration(password: String, environment: Environment) =
    pipewrenchService.buildConfiguration(databaseConf = environment.toDatabaseConfig(password),
                                         tableMetadata = environment.metadata,
                                         environment = environment.toPipewrenchEnvironment)

  def saveEnvironment(environment: Environment) =
    pipewrenchService.saveEnvironment(environment.toPipewrenchEnvironment)

}
