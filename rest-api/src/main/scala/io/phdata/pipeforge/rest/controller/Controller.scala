package io.phdata.pipeforge.rest.controller

import java.io.{ PrintWriter, StringWriter }
import java.util.Base64

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.ExceptionHandler
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.domain.{ JsonSupport, Status, YamlSupport }

import scala.util.{ Failure, Success, Try }

trait Controller extends LazyLogging with JsonSupport with YamlSupport {

  implicit def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      extractUri { uri =>
        logger.error(s"An Error occurred while processing the request", ex)
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))

        complete(
          InternalServerError,
          Status(status = "FAILURE", message = ex.getMessage, stacktrace = Some(sw.toString)))
      }
  }

  def decodePassword(request: HttpRequest): Try[String] =
    request.headers.find(_.is("password")) match {
      case Some(password) =>
        Success(Base64.getDecoder.decode(password.value()).map(_.toChar).mkString)
      case None =>
        Failure(new Exception("Base64 encoded `password` required as header attribute"))
    }

}
