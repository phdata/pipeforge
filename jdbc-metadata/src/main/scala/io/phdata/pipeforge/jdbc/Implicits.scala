package io.phdata.pipeforge.jdbc

import java.sql.ResultSet

object Implicits {

  implicit class ResultsetStream(resultSet: ResultSet) {
    def toStream: Stream[ResultSet] =
      new Iterator[ResultSet] {
        def hasNext: Boolean = resultSet.next()
        def next()           = resultSet
      }.toStream
  }
}
