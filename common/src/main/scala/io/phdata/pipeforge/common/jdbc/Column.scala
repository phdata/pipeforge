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

import java.sql.SQLType

/**
 * Column Definition
 * @param name Column name
 * @param comment Column comment
 * @param dataType SQL data type
 * @param nullable Is column nullable
 * @param index Column position
 * @param precision Data type precision
 * @param scale Data type scale
 */
case class Column(name: String,
                  comment: String,
                  dataType: SQLType,
                  nullable: Boolean,
                  index: Int,
                  precision: Int,
                  scale: Int) {

  /**
   * Determines whether the column is a decimal or not based on defined scale
   * @return
   */
  def isDecimal = scale > 0

}
