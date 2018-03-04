package io.phdata.pipeforge.rest.domain

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.phdata.pipeforge.jdbc.config.{DatabaseConf, DatabaseType, ObjectType}
import io.phdata.pipeforge.rest.domain.Model.Environment
import io.phdata.pipewrench.domain.{ColumnYaml, PipewrenchConfigYaml, TableYaml}
import spray.json.DefaultJsonProtocol

object Model {

  case class Environment(databaseType: String,
                         schema: String,
                         jdbcUrl: String,
                         username: String,
                         password: String,
                         objectType: String,
                         metadata: Map[String, String],
                         tables: Option[Seq[String]] = None)
}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val environmentFormat = jsonFormat8(Environment)

  implicit def columnFormat = jsonFormat5(ColumnYaml)
  implicit def tableFormat = jsonFormat7(TableYaml)
  implicit def pipewrenchConfigFormat = jsonFormat1(PipewrenchConfigYaml)

  def getDatabaseConf(environment: Environment): DatabaseConf = {
    val tables = environment.tables match {
      case Some(t) => Some(t.toSet)
      case None    => None
    }

    new DatabaseConf(
      databaseType = DatabaseType.withName(environment.databaseType),
      schema = environment.schema,
      jdbcUrl = environment.jdbcUrl,
      username = environment.username,
      password = environment.password,
      objectType = ObjectType.withName(environment.objectType),
      tables = tables
    )
  }
}
