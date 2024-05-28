package org.apache.spark.sql.hive

import io.delta.sql.DeltaSparkSessionExtension
import io.delta.sql.parser.DeltaSqlParser
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.hive.parser.CustomParser
import org.apache.spark.sql.hive.plan.{CustomDataSourceAnalyzer, CustomOptimizedPlan, CustomStrategy}

class CustomExtensionSuite extends DeltaSparkSessionExtension{

  override def apply(extensions: SparkSessionExtensions): Unit = {
    super.apply(extensions)
    extensions.injectParser { (session, parser) =>
      val delegate = new DeltaSqlParser(parser)
      new CustomParser(delegate)
    }

    extensions.injectResolutionRule(session => new CustomDataSourceAnalyzer(session) )
    extensions.injectOptimizerRule(CustomOptimizedPlan)
    extensions.injectPlannerStrategy(_ => CustomStrategy)

  }

}
