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

import java.io.{ PrintWriter, StringWriter }
import java.util.Base64

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.{ ExceptionHandler, MethodRejection, RejectionHandler }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.domain.{ Database, Environment, YamlSupport }
import io.phdata.pipewrench.domain.{ Column, Configuration, Kudu, Table }
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

import scala.util.{ Failure, Success, Try }

case class ErrorMessage(message: String, stacktrace: Option[String] = None)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def databaseJsonFormat     = jsonFormat2(Database)
  implicit def environmentJsonFormat  = jsonFormat14(Environment.apply)
  implicit def errorMessageJsonFormat = jsonFormat2(ErrorMessage)

  implicit def columnJsonFormat        = jsonFormat5(Column)
  implicit def kuduJsonFormat          = jsonFormat2(Kudu)
  implicit def tableJsonFormat         = jsonFormat8(Table)
  implicit def configurationJsonFormat = jsonFormat12(Configuration)
}

trait Handlers extends LazyLogging with JsonSupport with YamlSupport {

  implicit def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      extractUri { uri =>
        logger.error(s"An Error occurred while processing the request", ex)
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))

        println(sw.toString)
        complete(InternalServerError, ErrorMessage(ex.getMessage, Some(sw.toString)))
      }

  }

  implicit def rejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handleAll[MethodRejection] { rejections =>
        val supportedOptions = rejections.map(_.supported.name())
        complete(MethodNotAllowed,
                 s"Method not supported, options include: ${supportedOptions.mkString(",")}")
      }
      .handleNotFound {
        complete(NotFound, "The requested resource is not found")
      }
      .result()

  def decodePassword(request: HttpRequest): Try[String] =
    request.headers.find(_.is("password")) match {
      case Some(password) =>
        Success(Base64.getDecoder.decode(password.value()).map(_.toChar).mkString)
      case None =>
        Failure(new Exception("Base64 encoded `password` required as header attribute"))
    }

}
