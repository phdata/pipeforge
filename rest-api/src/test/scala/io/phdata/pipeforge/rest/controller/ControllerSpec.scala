package io.phdata.pipeforge.rest.controller

import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.phdata.pipeforge.rest.domain.Environment
import io.phdata.pipewrench.Pipewrench
import io.phdata.pipewrench.domain.{ Column, Configuration, Kudu, Table }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ Matchers, WordSpec }

/**
  * Controller test spec
  */
trait ControllerSpec
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with MockFactory
    with JsonSupport {

  // Mocked Pipewrench service
  lazy val pipewrenchService = mock[Pipewrench]

  // Test configuration
  val configuration = Configuration(
    name = "test.name",
    group = "test.group",
    user_name = "test.user_name",
    type_mapping = "test.type_mapping",
    sqoop_password_file = "test.sqoop_password_file",
    connection_manager = "test.conntection_manager",
    sqoop_job_name_suffix = "test.sqoop_job_name_suffix",
    source_database = Map("name"  -> "test.source_database.name"),
    staging_database = Map("name" -> "test.staging_database.name"),
    impala_cmd = "test.impal_cmd",
    tables = Seq(
      Table(
        id = "test.table.id",
        source = Map("name"      -> "test.table.source.name"),
        destination = Map("name" -> "test.table.source.name"),
        split_by_column = "test.table.split_by_column",
        primary_keys = Seq("test.table.primary_keys.col1"),
        kudu = Kudu(
          hash_by = List("col1"),
          num_partitions = 1
        ),
        metadata = Map("source" -> "test.table.metadata.source"),
        columns = Seq(
          Column(
            name = "test.table.columns.col1",
            datatype = "test.table.columns.datatype",
            comment = "test.table.column.comment"
          )
        )
      )
    )
  )

  // Test environment
  val environment = Environment(
    name = "test.name",
    group = "test.group",
    databaseType = "mysql", //must match enum value
    schema = "test.schema",
    jdbcUrl = "test.jdbcUrl",
    username = "test.username",
    objectType = "table", // must match enum value
    metadata = Map("source" -> "test.table.metadata.source"),
    hdfsPath = "test.hdfsPath",
    hadoopUser = "test.hadoopUser",
    passwordFile = "test.passwordFile",
    destinationDatabase = "test.destinationDatabase"
  )
}
