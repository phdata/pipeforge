package io.phdata.pipeforge.rest.service

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.jdbc.config.DatabaseConf
import io.phdata.pipeforge.rest.domain.{ Environment, Status }
import io.phdata.pipeforge.rest.module.ConfigurationModule
import io.phdata.pipewrench.PipewrenchImpl
import io.phdata.pipewrench.domain.Configuration
import io.phdata.pipeforge.rest.domain.Implicits._

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

trait PipewrenchService {

  def getConfiguration(databaseConf: DatabaseConf, environment: Environment): Try[Configuration]

  def saveConfiguration(configuration: Configuration): Status

  def saveEnvironment(environment: Environment): Status

  def executePipewrenchMerge(group: String, name: String, template: String): Status

}

class PipewrenchServiceImpl()(implicit executionContext: ExecutionContext)
    extends PipewrenchService
    with ConfigurationModule
    with LazyLogging {

  import sys.process._

  override def getConfiguration(databaseConf: DatabaseConf,
                                environment: Environment): Try[Configuration] =
    PipewrenchImpl.buildConfiguration(databaseConf,
                                      environment.metadata,
                                      environment.toPipewrenchEnvironment)

  override def saveConfiguration(configuration: Configuration): Status =
    status(Try {
      createIngestDirIfNotExist(configuration.group, configuration.name)

      PipewrenchImpl.writeYamlFile(
        configuration,
        s"${pipewrenchProjectDir(configuration.group, configuration.name)}/tables.yml")
    })

  override def saveEnvironment(environment: Environment): Status =
    status(Try {
      createIngestDirIfNotExist(environment.group, environment.name)

      PipewrenchImpl.writeYamlFile(
        environment.toPipewrenchEnvironment,
        s"${pipewrenchProjectDir(environment.group, environment.name)}/env.yml")
    })

  override def executePipewrenchMerge(group: String, name: String, template: String): Status =
    status(Try {
      val dir = pipewrenchProjectDir(group, name)
      s"pipewrench-merge --env $dir/env.yml --conf=$dir/tables.yml --pipeline-templates=$pipewrenchTemplateDir/$template" !!

      s"cp -R output $dir" !

      s"rm -rf output" !
    })

  private def status(proc: Try[Unit]): Status =
    proc match {
      case Success(_)  => Status("SUCCESS", "Everything is awesome!")
      case Failure(ex) => Status("FAILURE", ex.getMessage)
    }

  private def createIngestDirIfNotExist(group: String, name: String): Unit = {
    val dir = new File(pipewrenchProjectDir(group, name))
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

}
