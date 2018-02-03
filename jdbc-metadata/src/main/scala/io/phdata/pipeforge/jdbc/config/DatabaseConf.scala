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
 * Defines the database-configuration file
 *
 * @param databaseType Source database type can be 'oracle', 'mysql', or 'mssql'
 * @param schema       Schema or database to parse
 * @param jdbcUrl      JDBC connection string url
 * @param username     The username
 * @param password     The password
 * @param objectType   Database objects to parse can be 'table' or 'view'
 * @param tables       White listing of tables to parse
 */
case class DatabaseConf(databaseType: DatabaseType.Value,
                        schema: String,
                        jdbcUrl: String,
                        username: String,
                        password: String,
                        objectType: ObjectType.Value,
                        tables: Option[Set[String]] = None)
