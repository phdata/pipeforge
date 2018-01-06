package io.phdata.jdbc.domain

import java.sql.SQLType

/**
  * Column Definition
  * @param name Column name
  * @param dataType SQL data type
  * @param nullable Is column nullable
  * @param index Column position
  * @param precision Data type precision
  * @param scale Data type scale
  */
case class Column(name: String,
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
