package io.phdata.pipewrench.domain

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._

import scala.io.Source

case class TableMetadataYaml(metadata: Map[String, String])

object TableMetadataYamlProtocol extends DefaultYamlProtocol {

  implicit def tableMetadataFormat = yamlFormat1(TableMetadataYaml)

  def parseTablesMetadata(path: String) = {
    val file = Source.fromFile(path).getLines.mkString("\n")
    file.parseYaml.convertTo[TableMetadataYaml]
  }

}
