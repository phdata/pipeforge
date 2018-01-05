package io.phdata.jdbc

import java.sql.{DriverManager, ResultSet}

import com.spotify.docker.client.{DefaultDockerClient, DockerClient}
import com.typesafe.scalalogging.LazyLogging
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.{DockerCommandExecutor, DockerContainer, DockerContainerState, DockerFactory, DockerKit, DockerReadyChecker}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Try

trait DockerTestRunner extends FunSuite with DockerKit with BeforeAndAfterAll with LazyLogging {

  override val StartContainersTimeout = 10.minutes

  private val client: DockerClient = DefaultDockerClient.fromEnv().build()

  override implicit val dockerFactory: DockerFactory = new SpotifyDockerFactory(client)

  val image: String
  val advertisedPort: Int
  val exposedPort: Int
  val container: DockerContainer

  abstract override def dockerContainers: List[DockerContainer] = container :: super.dockerContainers

  protected def getResults[T](resultSet: ResultSet)(f: ResultSet => T) = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}

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
