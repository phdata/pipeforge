package io.phdata.jdbc.util

import scala.util.{Failure, Success, Try}

object ExceptionUtil {
  class MyTry[T](val t: Try[T]) {
    def messageOnFailure(message: String) = {
      t match {
        case Success(v) => Success(v)
        case Failure(e) => Failure(new Exception(message, e))
      }
    }
  }

  implicit def doTry[T](t: Try[T]) = new MyTry(t)
}
