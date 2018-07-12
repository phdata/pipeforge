package io.phdata.pipeforge.jdbc

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipeforge.common.{ AppConfiguration, Environment }

trait SchemaValidator {

  def validateSchema(environment: Environment, databasePassword: String, impalaPassword: String)

}

object SchemaValidator extends SchemaValidator with AppConfiguration with LazyLogging {

  override def validateSchema(environment: Environment,
                              databasePassword: String,
                              impalaPassword: String): Unit = {}

}
