package io.phdata.pipewrench.domain

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class PipewrenchConfigYaml(tables: Seq[TableYaml])

object PipewrenchConfigYamlProtocol extends DefaultYamlProtocol {
  import TableMetadataYamlProtocol._
  implicit def columnFormat           = yamlFormat5(ColumnYaml)
  implicit def tableFormat            = yamlFormat7(TableYaml)
  implicit def pipewrenchConfigFormat = yamlFormat1(PipewrenchConfigYaml)
}
