package io.phdata.pipeforge.jdbc

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.jdbc._
import io.phdata.pipeforge.common.{ AppConfiguration, Environment }

import scala.util.{ Failure, Success, Try }
import ai.x.diff.DiffShow
import ai.x.diff.conversions._

trait SchemaValidator {

  def validateSchema(environment: Environment, databasePassword: String, impalaPassword: String)

}

object SchemaValidator extends SchemaValidator with AppConfiguration with LazyLogging {

  override def validateSchema(environment: Environment, databasePassword: String, impalaPassword: String): Unit =
    getImpalaJdbcUrl(environment) match {
      case Success(impalaJdbcUrl) =>
        val sourceDatabaseConf = environment.toDatabaseConfig(databasePassword)
        val impalaDatabaseConf = DatabaseConf(
          DatabaseType.IMPALA,
          environment.rawDatabase.name,
          impalaJdbcUrl,
          environment.hadoopUser,
          impalaPassword,
          ObjectType.withName(environment.objectType)
        )

        logger.info(s"Validating schemas source: {}, impala: {}",
                    sourceDatabaseConf.copy(password = "******"),
                    impalaDatabaseConf.copy(password = "******"))
        DatabaseMetadataParser.parse(environment.toDatabaseConfig(databasePassword)) match {
          case Success(sourceTables) =>
            DatabaseMetadataParser.parse(impalaDatabaseConf) match {
              case Success(impalaTables) =>
                if (DiffShow[List[Table]].diffable(sourceTables, impalaTables)) {
                  val diff = DiffShow[List[Table]].diff(sourceTables, impalaTables)
                  print(diff.toString)
                  logger.info(s"Schema difference: $diff")
                }
              case Failure(ex) => logger.error("Failed to parse impala schema", ex)
            }
          case Failure(ex) => logger.error("Failed to parse source system schema", ex)
        }
      case Failure(ex) =>
        logger.error("Failed to build impala jdbc url from application.conf properties", ex)
    }

  private def getImpalaJdbcUrl(environment: Environment): Try[String] =
    impalaHostOpt match {
      case Some(impalaHost) =>
        impalaPortOpt match {
          case Some(impalaPort) =>
            Success(s"jdbc:hive2://$impalaHost:$impalaPort/${environment.rawDatabase.name};ssl=true;AuthMech=3")
          case None =>
            Failure(new Exception("`impala.port` in application.conf is required to do schema validation"))
        }
      case None =>
        Failure(new Exception("`impala.hostname` in application.conf is required to do schema validation"))
    }

}
