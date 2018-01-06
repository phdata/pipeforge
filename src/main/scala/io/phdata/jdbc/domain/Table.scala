package io.phdata.jdbc.domain

/**
  * Table definition
  * @param name Table name
  * @param primaryKeys A set of primary key definitions
  * @param columns A set of column definitions
  */
case class Table(name: String,
                 primaryKeys: Set[Column],
                 columns: Set[Column])
