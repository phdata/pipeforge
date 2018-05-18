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

package io.phdata.pipeforge.jdbc.config

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
  val SYBASE   = Value("sybase")

  def getConnectionManager(dbType: DatabaseType.Value): String =
    dbType match {
      case MYSQL    => "org.apache.sqoop.manager.MySQLManager"
      case ORACLE   => "org.apache.sqoop.manager.OracleManager"
      case MSSQL    => "org.apache.sqoop.manager.SQLServerManager"
      case HANA     => "org.apache.sqoop.manager.GenericjdbcManager"
      case TERADATA => "org.apache.sqoop.manager.GenericjdbcManager"
      case AS400    => "com.ibm.as400.access.AS400JDBCDriver"
      case SYBASE   => "com.sybase.jdbc4.jdbc.SybDriver"
      case _ =>
        throw new Exception(
          s"Database type: $dbType does not have valid Connection Manager mapping")
    }
}
