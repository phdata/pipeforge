package io.phdata.pipeforge.rest.controller

import java.util.Base64

import akka.http.scaladsl.model.HttpRequest

import scala.util.{Failure, Success, Try}

object Util {

  def decodePassword(request: HttpRequest): Try[String] = {
    request.headers.find(_.is("password")) match {
      case Some(password) =>
        Success(Base64.getDecoder.decode(password.value()).map(_.toChar).mkString)
      case None =>
        Failure(new Exception("Base64 encoded `password` required as header attribute"))
    }

  }

}
