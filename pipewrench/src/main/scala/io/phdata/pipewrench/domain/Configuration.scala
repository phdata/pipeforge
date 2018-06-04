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

package io.phdata.pipewrench.domain

/**
 * Configuration object
 * @param name
 * @param group
 * @param user_name
 * @param type_mapping
 * @param sqoop_password_file
 * @param connection_manager
 * @param sqoop_driver
 * @param sqoop_job_name_suffix
 * @param source_database
 * @param staging_database
 * @param impala_cmd
 * @param tables
 */
case class Configuration(name: String,
                         group: String,
                         user_name: String = "{{ source_db_user_name }}",
                         type_mapping: String = "type-mapping.yml",
                         sqoop_password_file: String = "{{ password_file }}",
                         connection_manager: Option[String],
                         sqoop_driver: Option[String],
                         sqoop_job_name_suffix: String,
                         source_database: Map[String, String],
                         staging_database: Map[String, String],
                         raw_database: Map[String, String],
                         impala_cmd: String,
                         tables: Seq[Table])
