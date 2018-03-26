package io.phdata.pipeforge

import io.phdata.pipeforge.Pipeforge.CliArgsParser
import org.scalatest.{ FunSuite, Matchers }

class PipeforgeTest extends FunSuite with Matchers {

  test("rest-api should be a subcommand") {
    val cliArgs = new CliArgsParser(Seq("rest-api", "-p", "1"))
    cliArgs.subcommand shouldEqual Some(cliArgs.restApi)
    cliArgs.restApi.port() shouldEqual 1
  }

  test("pipewrench should be a subcomannd") {
    val cliArgs = new CliArgsParser(
      Seq("pipewrench", "-e", "environment.yml", "-p", "password", "-o", "output-path"))
    cliArgs.subcommand shouldEqual Some(cliArgs.pipewrench)
    cliArgs.pipewrench.databaseConf() shouldEqual "environment.yml"
    cliArgs.pipewrench.databasePassword() shouldEqual "password"
    cliArgs.pipewrench.outputPath() shouldEqual "output-path"
  }
}
