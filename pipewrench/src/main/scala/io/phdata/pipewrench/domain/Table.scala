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
 * Table object
 * @param id
 * @param source
 * @param destination
 * @param split_by_column
 * @param primary_keys
 * @param kudu
 * @param columns
 * @param metadata
 */
case class Table(id: String,
                 source: Map[String, String],
                 destination: Map[String, String],
                 split_by_column: String,
                 primary_keys: Seq[String],
                 kudu: Kudu,
                 columns: Seq[Column],
                 metadata: Map[String, String],
                 comment: String = "")
