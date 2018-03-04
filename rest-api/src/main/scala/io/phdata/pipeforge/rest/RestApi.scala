package io.phdata.pipeforge.rest

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.module._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object RestApi extends LazyLogging {

  def start(): Unit = {
    val app = new AppModule with ExecutionContextModule with ConfigurationModule with ServiceModule with AkkaModule with HttpModule with RestModule

    import app.executionContext
    Await.ready(
      for {
        _ <- app.restApi.start()
      } yield Unit, Duration.Inf
    )
  }
}
