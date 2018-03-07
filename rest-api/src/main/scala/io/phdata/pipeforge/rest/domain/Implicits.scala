package io.phdata.pipeforge.rest.domain

import io.phdata.pipeforge.jdbc.config.{ DatabaseConf, DatabaseType, ObjectType }
import io.phdata.pipewrench.domain.{ Environment => PipewrenchEnvironment }

object Implicits {

  implicit class EnvironmentPipewrench(environment: Environment) {
    def toPipewrenchEnvironment: PipewrenchEnvironment =
      PipewrenchEnvironment(
        name = environment.name,
        group = environment.group,
        connection_string = environment.jdbcUrl,
        hdfs_basedir = environment.hdfsPath,
        hadoop_user = environment.hadoopUser,
        password_file = environment.passwordFile,
        destination_database = environment.destinationDatabase
      )
  }

  implicit class EnvironmentDatabaseConfig(environment: Environment) {
    def toDatabaseConfig(password: String): DatabaseConf =
      DatabaseConf(
        databaseType = DatabaseType.withName(environment.databaseType),
        schema = environment.schema,
        jdbcUrl = environment.jdbcUrl,
        username = environment.username,
        password = password,
        objectType = ObjectType.withName(environment.objectType),
        tables = environment.tables
      )
  }

}
