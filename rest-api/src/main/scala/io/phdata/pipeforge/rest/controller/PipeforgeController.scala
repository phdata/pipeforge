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

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.Directives._
import io.phdata.pipeforge.rest.domain.{ Environment, JsonSupport, YamlSupport }
import io.phdata.pipeforge.rest.service.PipewrenchService

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

import java.util.Base64

class PipeforgeController(pipewrenchService: PipewrenchService)(
    implicit executionContext: ExecutionContext)
    extends LazyLogging
    with YamlSupport
    with JsonSupport {

  val basePath = "pipeforge"

  val route =
    extractRequest { request =>
      path(basePath) {
        get {
          complete("Pipeforge rest api")
        } ~
        post {
          parameter('template) { template =>
            entity(as[Environment]) { environment =>
              Util.decodePassword(request) match {
                case Success(password) =>
                  pipewrenchService.getConfiguration(password, environment) match {
                    case Success(configuration) =>
                      pipewrenchService.saveEnvironment(environment)
                      pipewrenchService.saveConfiguration(configuration)
                      complete(
                        pipewrenchService.executePipewrenchMerge(environment.group,
                                                                 environment.name,
                                                                 template))
                    case Failure(ex) => failWith(ex)
                  }
                case Failure(ex) => failWith(ex)
              }
            }
          }
        }
      }
    }
}
