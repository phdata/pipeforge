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

package io.phdata.pipeforge.rest.module

import akka.actor.ActorSystem
import akka.http.scaladsl.{ Http, HttpExt }
import akka.http.scaladsl.server.Route
import akka.stream.{ ActorMaterializer, Materializer }
import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.controller.PipewrenchController
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import io.phdata.pipewrench.PipewrenchService

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Rest Module
 * Configures necessary dependencies for Akka Http endpoints
 */
trait RestModule {

  def pipewrenchService: PipewrenchService = new PipewrenchService()

  val config: Config = ConfigFactory.load()

  implicit val actorSystem: ActorSystem   = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  // Verify Pipewrench installation
  pipewrenchService.install()
  val pipewrenchController = new PipewrenchController(pipewrenchService)

  val http: HttpExt = Http()
  val restApi       = new RestApi(http, pipewrenchController)

}

class RestApi(http: HttpExt, pipewrenchController: PipewrenchController)(
    implicit actorSystem: ActorSystem,
    materializer: Materializer,
    executionContext: ExecutionContext)
    extends LazyLogging {

  val route: Route =
    cors() {
      pipewrenchController.route
    }

  def start(port: Int): Future[Unit] =
    http.bindAndHandle(route, "0.0.0.0", port = port) map { binding =>
      logger.info(s"Pipeforge Rest interface bound to ${binding.localAddress}")
    } recover {
      case ex =>
        logger.error(s"Pipeforge Rest interface could not bind", ex)
    }
}
