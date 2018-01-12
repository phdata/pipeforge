/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.jdbc.util

import java.io.{FileWriter, StringWriter}
import java.util

import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

/**
  * Utility for reading and writing YAML
  */
object YamlWrapper {
  val options = new DumperOptions()
  options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
  options.setIndent(2)
  val yaml = new Yaml(options)

  /**
    * Convert Scala objects to YAML then write to file
    * @param data
    * @param path
    */
  def write(data: Map[String, Object], path: String): Unit = {
    val sw = getYaml(data)
    write(sw, path)
  }

  /**
    * Get yaml from Scala objects
    * @param data
    * @return
    */
  def getYaml(data: Map[String, Object]): StringWriter = {
    val sw = new StringWriter()

    val javaData = toJava(data)
    yaml.dump(javaData, sw)
    return sw
  }

  /**
    * Read yaml into Scala objects
    * @param path
    * @return
    */
  def read(path: String): Map[String, Object] = {
    val f = Source.fromFile(path).getLines().mkString("\n")
    val javaYaml = yaml.load(f)
    toScala(javaYaml).asInstanceOf[mutable.HashMap[String, Object]].toMap
  }

  /**
    * Write a StringWrite to path
    * @param sw
    * @param path
    */
  private def write(sw: StringWriter, path: String): Unit = {
    val fw = new FileWriter(path)
    fw.write(sw.toString)
    fw.close()
  }

  /**
    * Convert nested Scala collections to nested Java collections for SnakeYaml compatibility
    * @param m
    * @return
    */
  private def toJava(m: Any): Any = {
    m match {
      case sm: Map[_, _] => sm.map(kv => (kv._1, toJava(kv._2))).asJava
      case sl: Iterable[_] =>
        new util.ArrayList(
          sl.map(toJava).asJava.asInstanceOf[util.Collection[_]])
      case _ => m
    }
  }

  /**
    * Convert nested Java collections to nested Scala collections for SnakeYaml compatibility
    * @param m
    * @return
    */
  private def toScala(m: Any): Any = {
    m match {
      case sm: util.Map[_, _] => sm.asScala.map(kv => (kv._1, toScala(kv._2)))
      case sl: util.ArrayList[_] =>
        Seq(
          sl.asScala.map(toScala).toSeq)
      case _ => m
    }
  }
}
