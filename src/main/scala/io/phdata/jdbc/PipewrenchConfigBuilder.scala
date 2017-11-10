package io.phdata.jdbc

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.phdata.jdbc.config.{DatabaseConf, PipewrenchConf}
import io.phdata.jdbc.domain.Table
import io.phdata.jdbc.parsing.DatabaseMetadataParser
import io.phdata.jdbc.pipewrench.TableBuilder
import io.phdata.jdbc.util.YamlWrapper

import scala.util.{Failure, Success}

/**
  * Query a source database via JDBC and output generated Pipewrench config
  * (`tables.yml`) for all tables in a given schema.
  */
object PipewrenchConfigBuilder extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("CWD: " + new File(".").getAbsolutePath())
    val sourceDbConf = DatabaseConf.parse("source-database.conf")
    val databaseMetadata = DatabaseMetadataParser.parse(sourceDbConf)

    val pipewrenchConf = PipewrenchConf.parse("pipewrench.conf")

    val initialConfData = YamlWrapper.read(pipewrenchConf.defaultSourceConfPath)
    val initialTableData =
      YamlWrapper.read(pipewrenchConf.defaultTablesConfPath)

    val generatedConfig = databaseMetadata match {
      case Success(v) =>
        buildPipewrenchConfig(initialConfData, initialTableData, v)

      case Failure(e) => throw e
    }

    YamlWrapper.write(generatedConfig, pipewrenchConf.outFile)
  }

  def buildPipewrenchConfig(initialConfData: Map[String, Object],
                            initialTableData: Map[String, Object],
                            tableMetadata: Set[Table]) = {
    val tables =
      TableBuilder.buildTablesSection(tableMetadata, initialTableData)

    initialConfData + ("tables" -> tables)
  }
}
