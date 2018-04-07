package io.phdata.pipeforge.rest

import io.phdata.pipeforge.rest.module.RestModule
import io.phdata.pipewrench.PipewrenchService

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RestApp(service: PipewrenchService) extends RestModule {

  override def pipewrenchService: PipewrenchService = service

  def start(port: Int) = Await.ready(restApi.start(port), Duration.Inf)

}
