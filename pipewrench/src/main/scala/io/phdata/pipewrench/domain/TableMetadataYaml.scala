package io.phdata.pipewrench.domain

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._

import scala.io.Source

case class TableMetadataYaml(META_SOURCE: String,
                             META_SECURITY_CLASSIFICATION: String,
                             META_LOAD_FREQUENCY: String,
                             META_CONTACT_INFO: String)

object TableMetadataYamlProtocol extends DefaultYamlProtocol {

  implicit def tableMetadataFormat = yamlFormat4(TableMetadataYaml)

  def parseTablesMetadata(path: String) = {
    val file = Source.fromFile(path).getLines.mkString("\n")
    file.parseYaml.convertTo[TableMetadataYaml]
  }

}
