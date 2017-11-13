package io.phdata.jdbc.pipewrench

import java.sql.JDBCType

import io.phdata.jdbc.domain.Table

object TableBuilder {
  def buildTablesSection(tableMetadata: Set[Table],
                         initialData: Map[String, Object]) = {
    val sorted = tableMetadata.toList.sortBy(_.name)
    sorted.map(t => buildTable(t) ++ initialData)
  }

  def buildTable(table: Table) = {
    val allColumns = table.primaryKeys.toSeq ++ table.columns.toSeq

    Map(
      "id" -> table.name,
      "source" ->
        Map("name" -> table.name),
      "split_by_column" -> getSplitByColumn(table),
      "destination" ->
        Map("name" -> table.name.toLowerCase),
      "columns" -> ColumnBuilder.buildColumns(allColumns),
      "primary_keys" -> table.primaryKeys.toList.map(_.name)
    )
  }

  def getSplitByColumn(table: Table) = {
    table.primaryKeys
      .filter(x => x.dataType == JDBCType.BIGINT)
      .headOption
      .orElse(
        table.primaryKeys.toList.headOption
      )
      .orElse(
        table.columns.toList.headOption
      )
      .get
      .name
  }
}
