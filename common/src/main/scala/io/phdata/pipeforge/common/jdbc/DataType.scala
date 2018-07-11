package io.phdata.pipeforge.common.jdbc

import java.sql.JDBCType

object DataType extends Enumeration {
  val BOOLEAN = Value("BOOLEAN")
  val DECIMAL = Value("DECIMAL")
  val BIG_INT = Value("BIGINT")
  val INTEGER = Value("INTEGER")
  val SHORT   = Value("SHORT")

  def mapDataType(column: Column): String =
    column match {
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s > 0 => DataType.DECIMAL.toString
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 19 =>
        DataType.DECIMAL.toString
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 10 =>
        DataType.BIG_INT.toString
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 5 =>
        DataType.INTEGER.toString
      case Column(_, _, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 3 => DataType.SHORT.toString
      case _                                                             => column.dataType.toString
    }
}
