package io.phdata.jdbc.domain

case class Table(name: String,
                 primaryKeys: Set[Column],
                 columns: Set[Column])
