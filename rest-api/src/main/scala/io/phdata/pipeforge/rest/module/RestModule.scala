package io.phdata.pipeforge.rest.module

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.controller.PipeforgeController
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

import scala.concurrent.{ExecutionContext, Future}

trait RestModule {
  this: AkkaModule with ExecutionContextModule with HttpModule with ConfigurationModule =>

  val pipeforgeController = new PipeforgeController()

  val restApi = new RestApi(http, configuration, pipeforgeController)

}

class RestApi(http: HttpExt, configuration: Config, pipeforgeController: PipeforgeController) (implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends LazyLogging {

  val route: Route =
    cors() {
      pipeforgeController.route
    }

  val port = configuration.getInt("rest.port")

  def start(): Future[Unit] = {
    http.bindAndHandle(route, "0.0.0.0", port = port) map {
      binding =>
        logger.info(s"Pipeforge Rest interface bound to ${binding.localAddress}")
    } recover {
      case ex =>
        logger.error(s"Pipeforge Rest interface could not bind", ex)
    }
  }
}