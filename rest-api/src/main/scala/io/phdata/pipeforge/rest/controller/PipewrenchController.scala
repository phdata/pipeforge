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
import io.phdata.pipeforge.common.{ Environment, YamlSupport }
import io.phdata.pipeforge.common.pipewrench.Configuration
import io.phdata.pipewrench.Pipewrench
import net.jcazevedo.moultingyaml._

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

/**
 * Pipewrench controller exposes rest endpoints to allow users to:
 *
 * Build Pipewrench Configuration from JDBC meta data.
 * Convert Pipeforge Environment into Pipewrench enviornment and write the file to configured directory.
 * Run the Pipewrench Merge command to produce output scripts based one specified template.
 *
 * All endpoints accept and produce either JSON or Yaml documents.
 *
 * @param pipewrenchService Pipewrench Service
 * @param executionContext Application execution context
 */
class PipewrenchController(pipewrenchService: Pipewrench)(
    implicit executionContext: ExecutionContext)
    extends Handlers
    with YamlSupport {

  // base path for all uris
  val basePath = "pipewrench"

  val route =
    handleExceptions(exceptionHandler) {
      extractRequest { request =>
        logger.debug(s"""
             |Method: ${request.method.value}
             |URI: ${request.uri}
             |Content Type: ${request.entity.contentType}
             |Headers: ${request.headers}
           """.stripMargin)
        // Basic GET endpoint displays a text message
        path(basePath) {
          get {
            complete(s"Pipewrench Rest Api")
          }
        } ~
        // POST http://<host>:<port>/pipewrench/merge?template=<template name>
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
        // PUT http://<host>:<port>/pipewrench/configuration
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
        // POST http://<host>:<port>/pipwrench/environment
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

  /**
   * Executes the Pipewrench merge command.  First saves the configuration to the configured directory then runs pipewrench.
   *
   * @param template A string containing only the template name
   * @param configuration The Pipewrench Configuration
   */
  def merge(template: String, configuration: Configuration) = {
    pipewrenchService.saveConfiguration(configuration)
    pipewrenchService.executePipewrenchMergeApi(template, configuration)
  }

  /**
   * Builds Pipewrench Configuration from JDBC metadata.
   *
   * @param password A base64 encoded string
   * @param environment The Pipeforge Environment
   * @return Configuration
   */
  def buildConfiguration(password: String, environment: Environment) =
    pipewrenchService.buildConfiguration(databaseConf = environment.toDatabaseConfig(password),
                                         tableMetadata = environment.metadata,
                                         environment = environment.toPipewrenchEnvironment)

  /**
   * Writes a Pipewrench Environment to the configured directory
   *
   * @param environment The Pipeforge Environment
   */
  def saveEnvironment(environment: Environment) =
    pipewrenchService.saveEnvironment(environment.toPipewrenchEnvironment)

}
