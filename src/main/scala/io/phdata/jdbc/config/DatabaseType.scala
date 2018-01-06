package io.phdata.jdbc.config

/**
  * Supported database types
  */
object DatabaseType extends Enumeration {

  val MYSQL = Value("mysql")
  val ORACLE = Value("oracle")
  val MSSQL = Value("mssql")

}
