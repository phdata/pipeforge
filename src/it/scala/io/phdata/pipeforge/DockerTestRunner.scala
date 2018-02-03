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

package io.phdata.pipeforge

import java.sql.{DriverManager, ResultSet}

import com.spotify.docker.client.{DefaultDockerClient, DockerClient}
import com.typesafe.scalalogging.LazyLogging
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.{DockerCommandExecutor, DockerContainer, DockerContainerState, DockerFactory, DockerKit, DockerReadyChecker}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Integration test Docker interface
  * https://github.com/whisklabs/docker-it-scala
  */
trait DockerTestRunner extends FunSuite with DockerKit with BeforeAndAfterAll with LazyLogging {

  // Container Properties
  val IMAGE: String
  val ADVERTISED_PORT: Int
  val EXPOSED_PORT: Int
  val CONTAINER: DockerContainer
  // Database Properties
  val PASSWORD: String
  val DATABASE: String
  val USER: String
  val TABLE: String
  val NO_RECORDS_TABLE: String
  val VIEW: String
  val URL: String
  val DRIVER: String

  override val StartContainersTimeout = 10.minutes

  private val client: DockerClient = DefaultDockerClient.fromEnv().build()

  override implicit val dockerFactory: DockerFactory = new SpotifyDockerFactory(client)

  abstract override def dockerContainers: List[DockerContainer] = CONTAINER :: super.dockerContainers

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}

/**
  * Helper function for determining if the docker container is running or not
  * @param driver
  * @param url
  * @param user
  * @param password
  * @param database
  * @param port
  */
class DatabaseReadyChecker(driver: String, url: String, user: String, password: String, database: String, port: Option[Int] = None) extends DockerReadyChecker {
  override def apply(container: DockerContainerState)(implicit docker: DockerCommandExecutor, ec: ExecutionContext) = {
    container
      .getPorts()
      .map(ports =>
        Try {
          Class.forName(driver)
          Option(DriverManager.getConnection(url, user, password)).map(_.close).isDefined
        }.getOrElse(false)
      )
  }
}