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
 * Environment object
 * @param name
 * @param group
 * @param connection_string
 * @param hdfs_basedir
 * @param hadoop_user
 * @param password_file
 */
case class Environment(name: String,
                       group: String,
                       connection_string: String,
                       hdfs_basedir: String,
                       hadoop_user: String,
                       password_file: String,
                       staging_database_path: String,
                       staging_database_name: String,
                       raw_database_path: String,
                       raw_database_name: String)
