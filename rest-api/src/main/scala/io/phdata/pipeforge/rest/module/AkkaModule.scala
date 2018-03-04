package io.phdata.pipeforge.rest.module

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

trait AkkaModule {
  this: ExecutionContextModule with ConfigurationModule with ServiceModule =>

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()

}
