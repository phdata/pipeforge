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

import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.Directives._
import io.phdata.pipeforge.rest.domain.{ Environment, JsonSupport }
import io.phdata.pipeforge.rest.service.PipewrenchService

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class PipewrenchController(pipewrenchService: PipewrenchService)(
    implicit executionContext: ExecutionContext)
    extends LazyLogging
    with JsonSupport {

  val route =
    path("pipewrench") {
      get {
        complete(s"Pipewrench Rest Api")
      } ~
      post {
        parameter('type.?) { responseType =>
          entity(as[Environment]) { environment =>
            val dbConf = getDatabaseConf(environment)
            pipewrenchService.buildConfig(dbConf, environment) match {
              case Success(config) =>
                responseType match {
                  case Some(t) =>
                    if (t == "yaml") {
                      complete(pipewrenchService.yaml(config))
                    } else {
                      complete(config)
                    }
                  case None =>
                    complete(config)
                }
              case Failure(ex) => failWith(ex)
            }
          }
        }
      }
    }

}
