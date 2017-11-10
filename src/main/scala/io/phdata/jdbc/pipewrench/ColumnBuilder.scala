package io.phdata.jdbc.pipewrench

import java.sql.JDBCType

import io.phdata.jdbc.domain.Column

object ColumnBuilder {
  def buildColumns(columns: Set[Column]) = {
    val sorted = columns.toList.sortBy(_.index)
    sorted.map(buildColumn)
  }

  def buildColumn(column: Column) = {
    val dataType = mapDataType(column)
    val map =
      Map("name" -> column.name, "datatype" -> dataType, "comment" -> "")

    if (dataType == "DECIMAL") {
      map + ("scale" -> column.scale) + ("precision" -> column.precision)
    } else {
      map
    }
  }

  def mapDataType(column: Column) = {
    column match {
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s > 0 => "DECIMAL"
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s > 0 => "DECIMAL"
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 19 =>
        "DECIMAL"
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 10 =>
        "BIGINT"
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 5 =>
        "INTEGER"
      case Column(_, JDBCType.NUMERIC, _, _, p, s) if s == 0 && p > 3 => "SHORT"
      case _ => column.dataType.toString
    }
  }
}
