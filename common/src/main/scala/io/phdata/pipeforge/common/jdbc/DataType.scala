package io.phdata.pipeforge.common.jdbc

import java.sql.JDBCType

object DataType extends Enumeration {

  def mapDataType(column: Column): JDBCType = {
    val dataType = column.dataType
    if (dataType == JDBCType.NUMERIC) {
      if (column.scale == 0) {
        JDBCType.DECIMAL
      } else if (column.precision > 19) {
        JDBCType.DECIMAL
      } else if (column.precision >= 10 && column.precision <= 19) {
        JDBCType.BIGINT
      } else if (column.scale == -127 && column.precision == 0) {
        // Oracle datatypes can be defined simply as NUMBER with no precision or scale
        // In this case the value can be either 123 or 1.23 so keep these as Strings
        JDBCType.VARCHAR
      } else {
        JDBCType.INTEGER
      }
    } else {
      dataType
    }
  }

}
