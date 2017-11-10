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
    Map(
      "id" -> table.name,
      "source" ->
        Map("name" -> table.name),
      "split_by_colummn" -> getSplitByColumn(table),
      "destination" ->
        Map("name" -> table.name.toLowerCase),
      "columns" -> ColumnBuilder.buildColumns(table.columns)
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
