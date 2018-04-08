package io.phdata.pipewrench.domain

/**
 * Kudu object
 * @param hash_by
 * @param num_partitions
 */
case class Kudu(hash_by: List[String], num_partitions: Int)
