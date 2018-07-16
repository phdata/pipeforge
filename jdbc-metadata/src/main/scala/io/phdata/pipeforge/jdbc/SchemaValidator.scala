package io.phdata.pipeforge.jdbc

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.jdbc.{DatabaseConf, DatabaseType, ObjectType, Table}
import io.phdata.pipeforge.common.{AppConfiguration, Environment}

import scala.util.{Failure, Success}

trait SchemaValidator {

  def validateSchema(environment: Environment, databasePassword: String, impalaPassword: String)

}

object SchemaValidator extends SchemaValidator with AppConfiguration with LazyLogging {

  override def validateSchema(environment: Environment,
                              databasePassword: String,
                              impalaPassword: String): Unit = {

    val sourceDatabaseConf = environment.toDatabaseConfig(databasePassword)
    val impalaDatabaseConf = DatabaseConf(
      DatabaseType.IMPALA,
      environment.rawDatabase.name,
      getImpalaJdbcUrl(environment),
      environment.hadoopUser,
      impalaPassword,
      ObjectType.withName(environment.objectType)
    )

    logger.info(s"Validating schemas source: {}, impala: {}", sourceDatabaseConf.copy(password = "******"), impalaDatabaseConf.copy(password = "******"))
    DatabaseMetadataParser.parse(environment.toDatabaseConfig(databasePassword)) match {
      case Success(sourceTables) =>
        DatabaseMetadataParser.parse(impalaDatabaseConf) match {
          case Success(impalaTables) => diffSchemas(sourceTables, impalaTables)
          case Failure(ex) => logger.error("Failed to parse impala schema", ex)
        }
      case Failure(ex) => logger.error("Failed to parse source system schema", ex)
    }
  }

  private def diffSchemas(source: List[Table], destination: List[Table]): Unit = {
    logger.debug(s"Diffing schemas source: $source, destination: $destination")
  }

  private def getImpalaJdbcUrl(environment: Environment): String =
    s"jdbc:hive2://$impalaHost:$impalaPort/${environment.rawDatabase.name};ssl=true;AuthMech=3"

}
