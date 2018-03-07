package io.phdata.pipewrench.domain

case class Environment(name: String,
                       group: String,
                       connection_string: String,
                       hdfs_basedir: String,
                       hadoop_user: String,
                       password_file: String,
                       destination_database: String)
