package io.phdata.pipeforge.rest

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.rest.module._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends LazyLogging {

  val app = new AppModule with ExecutionContextModule with ConfigurationModule with AkkaModule with HttpModule with RestModule

  import app.executionContext

  def main(args: Array[String]): Unit = {
    Await.ready(
      for {
        _ <- app.restApi.start()
      } yield Unit, Duration.Inf
    )
  }
}
