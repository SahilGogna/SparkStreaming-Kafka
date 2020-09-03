package com.bigdata.hadoop

import com.bigdata.constants.HadoopConstants

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopConfiguration extends HadoopConstants {
  // making a configuration object
  val config = new Configuration()

  // adding configuration files of the cluster to the config object
  config.addResource(new Path(CONFIG_PATH_1))
  config.addResource(new Path(CONFIG_PATH_2))

  val fileSystem: FileSystem = FileSystem.get(config)
}
