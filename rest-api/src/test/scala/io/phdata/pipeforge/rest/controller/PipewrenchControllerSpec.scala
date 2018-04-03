package io.phdata.pipeforge.rest.controller

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import io.phdata.pipeforge.rest.domain.YamlSupport
import io.phdata.pipewrench.domain.{ YamlSupport => PipewrenchYamlSupport }

import scala.util.Success

class PipewrenchControllerSpec extends ControllerSpec with YamlSupport with PipewrenchYamlSupport {

  import spray.json._
  import net.jcazevedo.moultingyaml._

  lazy val mockPipewrenchController = new PipewrenchController(mockPipewrenchService)

  "Pipewrench API" should {
    "return message on GET endpoint" in {
      Get(s"/${mockPipewrenchController.basePath}") ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "execute Pipewrench merge" in {
      (mockPipewrenchService.executePipewrenchMerge _).expects(*, *, *).returning(mockStatus)
      Post(
        s"/${mockPipewrenchController.basePath}/merge?group=test.group&name=test.name&template=test.template") ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "save Yaml configuration" in {
      (mockPipewrenchService.saveConfiguration _).expects(*).returning(mockStatus)
      val yaml = mockConfiguration.toYaml.prettyPrint
      Post(s"/${mockPipewrenchController.basePath}/configuration", yaml) ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "save JSON configuration" in {
      (mockPipewrenchService.saveConfiguration _).expects(*).returning(mockStatus)
      val json = mockConfiguration.toJson.prettyPrint
      Post(s"/${mockPipewrenchController.basePath}/configuration", json) ~> RawHeader(
        "Content-Type",
        ContentTypes.`application/json`.toString()) ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "generate configuration from Yaml environment" in {
      (mockPipewrenchService.getConfiguration _).expects(*, *).returning(Success(mockConfiguration))
      val yaml = mockEnvironment.toYaml.prettyPrint
      Put(s"/${mockPipewrenchController.basePath}/configuration", yaml) ~> RawHeader(
        "password",
        "cGFzcw==") ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "generate configuration from Json environment" in {
      (mockPipewrenchService.getConfiguration _).expects(*, *).returning(Success(mockConfiguration))
      val json = mockEnvironment.toJson.prettyPrint
      Put(s"/${mockPipewrenchController.basePath}/configuration", json) ~> RawHeader(
        "Content-Type",
        ContentTypes.`application/json`
          .toString()) ~> RawHeader("password", "cGFzcw==") ~> mockPipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
  }
}
