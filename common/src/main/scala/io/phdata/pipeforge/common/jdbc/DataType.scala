package io.phdata.pipeforge.common.jdbc

import java.sql.JDBCType

object DataType extends Enumeration {

  def mapDataType(column: Column): JDBCType =
    column match {
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s > 0               => JDBCType.DECIMAL
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 19    => JDBCType.DECIMAL
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 10    => JDBCType.BIGINT
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 5     => JDBCType.INTEGER
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 3     => JDBCType.INTEGER
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == -127 && p == 0 => JDBCType.VARCHAR
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s < 0 && p == 0     => JDBCType.INTEGER
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s)                        => JDBCType.INTEGER
      case Column(_, _, JDBCType.CHAR, _, _, p, s)                           => JDBCType.VARCHAR
      case Column(_, _, JDBCType.NCHAR, _, _, p, s)                          => JDBCType.VARCHAR
      case _                                                                 => column.dataType
    }

}
