package org.apache.spark.sql.hive

import io.delta.sql.DeltaSparkSessionExtension
import io.delta.sql.parser.DeltaSqlParser
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.hive.parser.CustomParser

class CustomExtensionSuite extends DeltaSparkSessionExtension{

  override def apply(extensions: SparkSessionExtensions): Unit = {
    super.apply(extensions)
    extensions.injectParser { (session, parser) =>
      val delegate = new DeltaSqlParser(parser)
      new CustomParser(delegate)

    }
  }

}
