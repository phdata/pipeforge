package io.phdata.pipeforge.rest.module

import io.phdata.pipeforge.rest.service.{PipewrenchService, PipewrenchServiceImpl}

trait ServiceModule {
  this: AkkaModule with ExecutionContextModule with ConfigurationModule with HttpModule =>

  val pipewrenchService: PipewrenchService = new PipewrenchServiceImpl()


}
