package io.phdata.pipeforge.rest.service

import io.phdata.pipeforge.jdbc.config.DatabaseConf
import io.phdata.pipeforge.rest.domain.Environment
import io.phdata.pipewrench.PipewrenchImpl
import io.phdata.pipewrench.domain.PipewrenchConfig

import scala.concurrent.ExecutionContext
import scala.util.Try

trait PipewrenchService {

  def buildConfig(databaseConf: DatabaseConf, environment: Environment): Try[PipewrenchConfig]

  def yaml(pipewrenchConfig: PipewrenchConfig): String

}

class PipewrenchServiceImpl()(implicit executionContext: ExecutionContext)
    extends PipewrenchService {

  override def buildConfig(databaseConf: DatabaseConf,
                           environment: Environment): Try[PipewrenchConfig] =
    PipewrenchImpl.buildConfig(databaseConf, environment.metadata)

  override def yaml(pipewrenchConfig: PipewrenchConfig): String =
    PipewrenchImpl.yamlStr(pipewrenchConfig)

}
