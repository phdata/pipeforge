package io.phdata.pipeforge.rest.domain

import io.phdata.pipewrench.domain.{ Column, Configuration, Table }
import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._

import scala.io.Source

trait YamlSupport extends DefaultYamlProtocol {

  implicit def columnYamlFormat        = yamlFormat5(Column)
  implicit def tableYamlFormat         = yamlFormat7(Table)
  implicit def configurationYamlFormat = yamlFormat10(Configuration)

  implicit def environmentYamlFormat = yamlFormat13(Environment)

  /**
   * Parses input file into Environment object
   * @param path
   * @return
   */
  def parseFile(path: String): Environment = {
    val file = Source.fromFile(path).getLines.mkString("\n")
    file.parseYaml.convertTo[Environment]
  }
}
