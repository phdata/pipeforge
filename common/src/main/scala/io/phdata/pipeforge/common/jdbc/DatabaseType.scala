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

package io.phdata.pipeforge.common.jdbc

/**
 * Supported database types
 */
object DatabaseType extends Enumeration {

  val MYSQL    = Value("mysql")
  val ORACLE   = Value("oracle")
  val MSSQL    = Value("mssql")
  val HANA     = Value("hana")
  val TERADATA = Value("teradata")
  val AS400    = Value("as400")
  val REDSHIFT = Value("redshift")
  val IMPALA   = Value("impala")

  def getDriver(dbType: DatabaseType.Value): Option[String] =
    dbType match {
      case MYSQL    => Some("com.mysql.jdbc.Driver")
      case ORACLE   => Some("oracle.jdbc.OracleDriver")
      case MSSQL    => Some("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      case HANA     => Some("com.sap.db.jdbc.Driver")
      case TERADATA => Some("com.teradata.jdbc.TeraDriver")
      case AS400    => Some("com.ibm.as400.access.AS400JDBCDriver")
      case REDSHIFT => Some("com.amazon.redshift.jdbc41.Driver")
      case IMPALA   => Some("org.apache.hive.jdbc.HiveDriver")
      case _        => None
    }

  def getConnectionManager(dbType: DatabaseType.Value): Option[String] =
    dbType match {
      case MYSQL  => Some("org.apache.sqoop.manager.MySQLManager")
      case ORACLE => Some("org.apache.sqoop.manager.OracleManager")
      case MSSQL  => Some("org.apache.sqoop.manager.SQLServerManager")
      case _      => None
    }
}
