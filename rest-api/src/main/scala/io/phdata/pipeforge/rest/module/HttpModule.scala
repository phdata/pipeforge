package io.phdata.pipeforge.rest.module

import akka.http.scaladsl.{Http, HttpExt}

trait HttpModule {
  this: AkkaModule =>

  val http: HttpExt = Http()

}
