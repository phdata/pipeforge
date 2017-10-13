package io.phdata.jdbc.domain

case class Configuration(databaseType: String,
                         schema: String,
                         jdbcUrl: String,
                         username: String,
                         password: String,
                         tables: Seq[String])
