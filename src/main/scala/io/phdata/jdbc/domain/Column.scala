package io.phdata.jdbc.domain

import java.sql.SQLType

case class Column(name: String, dataType: SQLType, nullable: Boolean, index: Int, precision: Int, scale: Int)
