package io.phdata.pipeforge.rest.controller

import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext

class PipeforgeController()(implicit executionContext: ExecutionContext) extends LazyLogging {

  val route = {
    path("pipeforge") {
      get {
        complete(s"Pipeforge Rest Api")
      }
    }
  }

}
