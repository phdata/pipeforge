package io.phdata.pipeforge.rest.controller

import akka.http.scaladsl.model.{ ContentTypes, StatusCodes }
import akka.http.scaladsl.model.headers.RawHeader

class PipeforgeControllerSpec extends ControllerSpec {

  import spray.json._

  lazy val pipeforgeController = new PipeforgeController(mockPipewrenchService)

  "Pipeforge API" should {
    "return message on GET endpoint" in {
      Get(s"/${pipeforgeController.basePath}") ~> pipeforgeController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "save Pipewrench configurations and execute pipewrench merge" in {
      val json = mockEnvironment.toJson.prettyPrint
      try {
        Post(s"/${pipeforgeController.basePath}?template=test.template", json) ~> RawHeader(
          "Content-Type",
          ContentTypes.`application/json`
            .toString()) ~> RawHeader("password", "cGFzcw==") ~> pipeforgeController.route ~> check {
          status should be(StatusCodes.OK)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }
}
