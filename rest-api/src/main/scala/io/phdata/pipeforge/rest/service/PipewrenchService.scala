package io.phdata.pipeforge.rest.service

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.jdbc.DatabaseMetadataParser
import io.phdata.pipeforge.jdbc.config.DatabaseConf
import io.phdata.pipeforge.rest.domain.Model.Environment
import io.phdata.pipewrench.Pipewrench
import io.phdata.pipewrench.domain.{PipewrenchConfigYaml, TableMetadataYaml}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

trait PipewrenchService {

  def buildConfig(dbConf: DatabaseConf, environment: Environment): Try[PipewrenchConfigYaml]

}

class PipewrenchServiceImpl()(implicit executionContext: ExecutionContext) extends PipewrenchService with LazyLogging {
  override def buildConfig(dbConf: DatabaseConf, environment: Environment): Try[PipewrenchConfigYaml] = {
    logger.info(s"pipewrench service $dbConf, $environment")
    DatabaseMetadataParser.parse(dbConf) match {
      case Success(tables) =>
        Try(Pipewrench.buildConfig(tables, TableMetadataYaml(environment.metadata)))
      case Failure(ex) => Failure(ex)
    }

  }
}
