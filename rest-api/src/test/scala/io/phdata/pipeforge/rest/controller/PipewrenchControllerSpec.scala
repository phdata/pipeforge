package io.phdata.pipeforge.rest.controller

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import io.phdata.pipeforge.common.YamlSupport
import io.phdata.pipeforge.common.pipewrench.Configuration

import scala.util.Success

/**
 * Pipewrench endpoint tests
 */
class PipewrenchControllerSpec extends ControllerSpec with YamlSupport {

  import spray.json._
  import net.jcazevedo.moultingyaml._

  lazy val pipewrenchController = new PipewrenchController(pipewrenchService)

  "Pipewrench API" should {
    "return message on GET endpoint" in {
      Get(s"/${pipewrenchController.basePath}") ~> pipewrenchController.route ~> check {
        status should be(StatusCodes.OK)
      }
    }
    "execute Pipewrench merge" in {
      (pipewrenchService.saveConfiguration _).expects(*).returning(Unit)
      (pipewrenchService.executePipewrenchMergeApi _).expects(*, *).returning(Unit)
      val yaml = configuration.toYaml.prettyPrint
      Post(s"/${pipewrenchController.basePath}/merge?template=test.template", yaml) ~> pipewrenchController.route ~> check {
        status should be(StatusCodes.Created)
      }
    }
    "generate configuration from Yaml environment" in {
      (pipewrenchService.buildConfiguration _).expects(*, *, *).returning(Success(configuration))
      val yaml = environment.toYaml.prettyPrint
      Put(s"/${pipewrenchController.basePath}/configuration", yaml) ~>
      RawHeader("password", "cGFzcw==") ~>
      pipewrenchController.route ~>
      check {
        status should be(StatusCodes.OK)
      }
    }
    "generate configuration from Json environment" in {
      (pipewrenchService.buildConfiguration _).expects(*, *, *).returning(Success(configuration))
      val json = environment.toJson.prettyPrint
      Put(s"/${pipewrenchController.basePath}/configuration", json) ~>
      RawHeader("Content-Type", ContentTypes.`application/json`.toString()) ~>
      RawHeader("password", "cGFzcw==") ~>
      pipewrenchController.route ~>
      check {
        status should be(StatusCodes.OK)
      }
    }
    "save Pipewrench environment from yaml" in {
      (pipewrenchService.saveEnvironment _).expects(*).returning(Unit)
      val yaml = environment.toYaml.prettyPrint
      Post(s"/${pipewrenchController.basePath}/environment", yaml) ~>
      pipewrenchController.route ~>
      check {
        status should be(StatusCodes.Created)
      }
    }
    "save Pipewrench environment from json" in {
      (pipewrenchService.saveEnvironment _).expects(*).returning(Unit)
      val json = environment.toJson.prettyPrint
      Post(s"/${pipewrenchController.basePath}/environment", json) ~>
      RawHeader("Content-Type", ContentTypes.`application/json`.toString()) ~>
      pipewrenchController.route ~>
      check {
        status should be(StatusCodes.Created)
      }
    }
  }
}
