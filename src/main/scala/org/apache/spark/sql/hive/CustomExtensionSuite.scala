package org.apache.spark.sql.hive

import io.delta.sql.DeltaSparkSessionExtension
import io.delta.sql.parser.DeltaSqlParser
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation
import org.apache.spark.sql.hive.customnativefunctions.{CustomAdd, Fibo, FiboFuncIn, FiboIter, ModelFunc}
import org.apache.spark.sql.hive.parser.CustomParser
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ResolveCatalogViews
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser
import org.apache.spark.sql.hive.plan.{CustomDataSourceAnalyzer, CustomOptimizedPlan, CustomStrategy, DescribeUnResolvedRelation, DescribeViewRelationRule, RowLevelFilter}

class CustomExtensionSuite extends DeltaSparkSessionExtension{

  override def apply(extensions: SparkSessionExtensions): Unit = {

    super.apply(extensions)
    extensions.injectParser { (session, parser) =>
     // val delegate = new DeltaSqlParser(parser)
     // new CustomParser(delegate)
      CustomSparkSQLParser
    }
    extensions.injectResolutionRule(session => new ResolveCatalogViews(session))
    extensions.injectResolutionRule(session => new DescribeUnResolvedRelation(session))
    extensions.injectResolutionRule(session => new DescribeViewRelationRule(session))
   // extensions.injectResolutionRule(session => new RowLevelFilter(session))
    extensions.injectResolutionRule(session => new CustomDataSourceAnalyzer(session) )

    extensions.injectOptimizerRule(CustomOptimizedPlan)
    extensions.injectPlannerStrategy(_ => CustomStrategy)
    extensions.injectFunction(CustomAdd.fd)
    extensions.injectFunction(Fibo.fd)
    extensions.injectFunction(ModelFunc.fd)
    extensions.injectFunction(FiboIter.fd)
    extensions.injectFunction(FiboFuncIn.fd)
   // super.apply(extensions)



  }

}
