package io.phdata.jdbc

import io.phdata.jdbc.config.Configuration
import io.phdata.jdbc.parsing.DatabaseMetadataParser


object ParserMain {
  def main(args: Array[String]) = {
    val configuration = Configuration.parse("application.conf")
    val results = DatabaseMetadataParser.parse(configuration)
  }
}
