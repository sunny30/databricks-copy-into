package org.apache.spark.sql.hive.classloader

import org.apache.hadoop.conf.Configuration

import java.net.URL
import java.io.File


class IsolatedClassPathLoader(paths:Seq[String], hadoopConf: Configuration) {


  def pathURL : Seq[URL] = paths.map(p => {
    (new File(p)).toURI.toURL
  })

  val rootClassLoader: ClassLoader = classOf[ClassLoader].getMethod("getPlatformClassLoader").
    invoke(null).asInstanceOf[ClassLoader]





}
