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
 * Table definition
 *
 * @param name Table name
 * @param comment Table comment
 * @param primaryKeys A set of primary key definitions
 * @param columns A set of column definitions
 */
case class Table(name: String, comment: String, primaryKeys: Set[Column], columns: Set[Column])
