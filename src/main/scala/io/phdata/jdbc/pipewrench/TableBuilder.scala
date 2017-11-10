package io.phdata.jdbc.pipewrench

import io.phdata.jdbc.domain.Table

object TableBuilder {
  def buildTablesSection(tableMetadata: Set[Table],
                         initialData: Map[String, Object]) = {
    val sorted = tableMetadata.toList.sortBy(_.name)
    sorted.map(buildTable)
  }

  def buildTable(table: Table) = {
    Map(
      "id" -> table.name,
      "source" ->
        Map("name" -> table.name),
      "destination" ->
        Map("name" -> table.name.toLowerCase),
      "columns" -> ColumnBuilder.buildColumns(table.columns)
    )
  }
}
