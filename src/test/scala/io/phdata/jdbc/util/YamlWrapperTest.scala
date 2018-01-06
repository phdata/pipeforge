package io.phdata.jdbc.util

import org.scalatest.FunSuite

/**
  * YamlWrapper unit tests
  */
class YamlWrapperTest extends FunSuite {
  test("write yaml") {
    val data = Map("key" -> "value",
      "list" ->
        Seq(Map("one" -> "two")
          , Map("three" -> "four")))


    YamlWrapper.write(data, "target/test.yml")
  }

  test("read yaml") {
    val data = Map("key" -> "value",
      "list" ->
        Seq(Map("one" -> "two")
          , Map("three" -> "four")))


    YamlWrapper.write(data, "target/test.yml")
    val str = YamlWrapper.read("target/test.yml")
    assert(str != null)
  }
}
