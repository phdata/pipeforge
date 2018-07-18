package io.phdata.pipeforge.jdbc

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.jdbc.{ DatabaseConf, DatabaseType, ObjectType, Table }
import io.phdata.pipeforge.common.{ AppConfiguration, Environment }

import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Success }

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

    logger.info(s"Validating schemas source: {}, impala: {}",
                sourceDatabaseConf.copy(password = "******"),
                impalaDatabaseConf.copy(password = "******"))
    DatabaseMetadataParser.parse(environment.toDatabaseConfig(databasePassword)) match {
      case Success(sourceTables) =>
        DatabaseMetadataParser.parse(impalaDatabaseConf) match {
          case Success(impalaTables) => diffSchemas(sourceTables, impalaTables)
          case Failure(ex)           => logger.error("Failed to parse impala schema", ex)
        }
      case Failure(ex) => logger.error("Failed to parse source system schema", ex)
    }
  }

  private def diffSchemas(source: List[Table], destination: List[Table]): Unit = {
    logger.debug(s"Diffing schemas source: $source, destination: $destination")
    val errors = diffTables(source, destination)
    if (errors.nonEmpty) {
      logger.error(s"Source schema does not match Impala schema, mismatches include:")
      errors.foreach(e => logger.error(e))
    }
  }

  private def diffTables(source: List[Table], destination: List[Table]): List[String] = {
    var errors = new ListBuffer[String]()
    source.foreach { sourceTable =>
      destination.find(_.name.toUpperCase == sourceTable.name.toUpperCase) match {
        case Some(destinationTable) =>
          sourceTable.columns.foreach { sourceColumn =>
            destinationTable.columns.find(_.name.toUpperCase == sourceColumn.name.toUpperCase) match {
              case Some(destinationColumn) =>
                // TODO: Add type-mapping.yml logic here
                // Example oracle NUMBER fields can either be integers or decimals
                if (destinationColumn.dataType != sourceColumn.dataType) {
                  errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} data type: ${sourceColumn.dataType}, does not match in impala, ${destinationColumn.dataType}"
                }
                // TODO: Enhance pipewrench templates to add nullable to fields that are
//                if (destinationColumn.nullable != sourceColumn.nullable) {
//                  errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} nullable: ${sourceColumn.nullable}, does not match in impala, ${destinationColumn.nullable}"
//                }
                // Necessary?
//                if (destinationColumn.comment != sourceColumn.comment) {
//                  errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} comment: ${sourceColumn.comment}, does not match in impala, ${destinationColumn.comment}"
//                }
                if (destinationColumn.isDecimal) {
                  if (destinationColumn.scale != sourceColumn.scale) {
                    errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} scale: ${sourceColumn.scale}, does not match in impala, ${destinationColumn.scale}"
                  }
                  if (destinationColumn.precision != sourceColumn.precision) {
                    errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} precision: ${sourceColumn.precision}, does not match in impala ${destinationColumn.precision}"
                  }
                }
              case None =>
                errors += s"Source column: ${sourceTable.name}.${sourceColumn.name} is not found in impala schema"
            }
          }
        case None => errors += s"Source table: ${sourceTable.name} is not found in impala schema"
      }
    }
    errors.toList
  }

  private def getImpalaJdbcUrl(environment: Environment): String =
    s"jdbc:hive2://$impalaHost:$impalaPort/${environment.rawDatabase.name};ssl=true;AuthMech=3"

}
