package io.phdata.pipeforge.rest.domain

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

import net.jcazevedo.moultingyaml._

import scala.io.Source

trait YamlProtocol extends DefaultYamlProtocol {

  implicit def environmentFormat = yamlFormat13(Environment)

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
