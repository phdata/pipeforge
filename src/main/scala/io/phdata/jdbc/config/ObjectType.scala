package io.phdata.jdbc.config

/**
  * Supported Database objects
  */
object ObjectType extends Enumeration {
  val VIEW = Value("view")
  val TABLE = Value("table")
}
