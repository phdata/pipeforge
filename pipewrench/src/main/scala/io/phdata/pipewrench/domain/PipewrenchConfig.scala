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

case class PipewrenchConfig(name: String,
                            user_name: String = "{{ source_db_user_name }}",
                            type_mapping: String = "type-mapping.yml",
                            sqoop_password_file: String = "{{ password_file }}",
                            connection_manager: String,
                            sqoop_job_name_suffix: String,
                            source_database: Map[String, String],
                            staging_database: Map[String, String],
                            tables: Seq[Table])
