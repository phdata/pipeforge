package io.phdata.pipeforge.rest.controller

import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.Directives._
import io.phdata.pipeforge.rest.domain.JsonSupport
import io.phdata.pipeforge.rest.domain.Model.Environment
import io.phdata.pipeforge.rest.service.PipewrenchService

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class PipewrenchController(pipewrenchService: PipewrenchService)(implicit executionContext: ExecutionContext) extends LazyLogging with JsonSupport {

  val route =
    path("pipewrench") {
      get {
        complete(s"Pipewrench Rest Api")
      } ~
      post {
        entity(as[Environment]) {
          environment =>
            val dbConf = getDatabaseConf(environment)
            pipewrenchService.buildConfig(dbConf, environment) match {
              case Success(config) => complete(config)
              case Failure(ex) => failWith(ex)
            }
        }
      }
    }

}
