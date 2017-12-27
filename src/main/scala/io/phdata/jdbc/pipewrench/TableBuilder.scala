package io.phdata.jdbc.pipewrench

import java.sql.JDBCType

import io.phdata.jdbc.domain.Table

object TableBuilder {
  def buildTablesSection(tableMetadata: Set[Table],
                         initialData: Map[String, Object]) = {
    tableMetadata.toList
      .sortBy(_.name)
      .map(t => buildTable(t) ++ initialData)
  }

  def buildTable(table: Table) = {
    val allColumns = table.primaryKeys ++ table.columns

    Map(
      "id" -> table.name,
      "source" -> Map("name" -> table.name),
      "split_by_column" -> getSplitByColumn(table),
      "destination" -> Map("name" -> table.name.toLowerCase),
      "columns" -> ColumnBuilder.buildColumns(allColumns),
      "primary_keys" -> table.primaryKeys.map(_.name)
    )
  }

  def getSplitByColumn(table: Table) = {
    table.primaryKeys.find(x => x.dataType == JDBCType.BIGINT)
      .orElse(table.primaryKeys.headOption)
      .orElse(table.columns.headOption)
      .get
      .name
  }
}
