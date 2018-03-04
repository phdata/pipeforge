package io.phdata.pipeforge.rest.module

import scala.concurrent.ExecutionContext

trait ExecutionContextModule {

  implicit val executionContext: ExecutionContext = ExecutionContext.global

}
