package io.phdata.pipeforge.rest.module

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigurationModule {

  val configuration: Config = ConfigFactory.load()

}
