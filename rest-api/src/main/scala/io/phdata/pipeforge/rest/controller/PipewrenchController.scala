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

import akka.http.scaladsl.model.ContentTypes
import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.Directives._
import io.phdata.pipeforge.rest.domain.{ Environment, JsonSupport, YamlSupport }
import io.phdata.pipeforge.rest.service.PipewrenchService
import io.phdata.pipewrench.domain.Configuration
import io.phdata.pipewrench.domain.{ YamlSupport => PipewrenchYamlSupport }
import net.jcazevedo.moultingyaml._

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class PipewrenchController(pipewrenchService: PipewrenchService)(
    implicit executionContext: ExecutionContext)
    extends LazyLogging
    with YamlSupport
    with PipewrenchYamlSupport
    with JsonSupport {

  val basePath = "pipewrench"

  val route =
    extractRequest { request =>
      path(basePath) {
        get {
          complete(s"Pipewrench Rest Api")
        }
      } ~
      path(basePath / "merge") {
        post {
          parameter('group, 'name, 'template) { (group, name, template) =>
            complete(pipewrenchService.executePipewrenchMerge(group, name, template))
          }
        }
      } ~
      path(basePath / "configuration") {
        post {
          request.entity.contentType match {
            case ContentTypes.`application/json` =>
              entity(as[Configuration]) { configuration =>
                complete(pipewrenchService.saveConfiguration(configuration))
              }
            case _ =>
              entity(as[String]) { yamlStr =>
                val configuration = yamlStr.parseYaml.convertTo[Configuration]
                complete(pipewrenchService.saveConfiguration(configuration))
              }
          }
        } ~
        put {
          Util.decodePassword(request) match {
            case Success(password) =>
              request.entity.contentType match {
                case ContentTypes.`application/json` =>
                  entity(as[Environment]) { environment =>
                    pipewrenchService.getConfiguration(password, environment) match {
                      case Success(configuration) => complete(configuration)
                      case Failure(ex)            => failWith(ex)
                    }
                  }
                case _ =>
                  entity(as[String]) { yamlStr =>
                    val environment = yamlStr.parseYaml.convertTo[Environment]
                    pipewrenchService.getConfiguration(password, environment) match {
                      case Success(configuration) => complete(configuration.toYaml.prettyPrint)
                      case Failure(ex)            => failWith(ex)
                    }
                  }
              }
            case Failure(ex) => failWith(ex)
          }
        }
      } ~
      path(basePath / "environment") {
        post {
          request.entity.contentType match {
            case ContentTypes.`application/json` =>
              entity(as[Environment]) { environment =>
                complete(pipewrenchService.saveEnvironment(environment))
              }
            case _ =>
              entity(as[String]) { yamlStr =>
                complete(
                  pipewrenchService.saveEnvironment(yamlStr.parseYaml.convertTo[Environment]))
              }
          }
        }
      }
    }
}
