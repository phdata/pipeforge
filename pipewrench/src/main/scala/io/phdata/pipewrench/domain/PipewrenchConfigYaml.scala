package io.phdata.pipewrench.domain

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class PipewrenchConfigYaml(tables: Seq[TableYaml])

object PipewrenchConfigYamlProtocol extends DefaultYamlProtocol {
  implicit def columnFormat = yamlFormat5(ColumnYaml)
  implicit def tableFormat  = yamlFormat6(TableYaml)
  implicit def pipewrenchConfigFormat = yamlFormat1(PipewrenchConfigYaml)
}