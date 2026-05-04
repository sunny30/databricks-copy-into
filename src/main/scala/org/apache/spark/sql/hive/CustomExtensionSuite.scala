package org.apache.spark.sql.hive

import io.delta.sql.DeltaSparkSessionExtension
import io.delta.sql.parser.DeltaSqlParser
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.analysis.{ProcedureArgumentCoercion, ResolveProcedures}
import org.apache.spark.sql.catalyst.optimizer.ReplaceStaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy
import org.apache.spark.sql.hive.customnativefunctions.{CustomAdd, Fibo, FiboFuncIn, FiboIter, ModelFunc}
import org.apache.spark.sql.hive.parser.CustomParser
import org.apache.spark.sql.hive.plan.listener.CatalogQueryExecutionListener
import org.apache.spark.sql.hive.plan.may26hack.ExternalCatalogCutAnalyzer
import org.apache.spark.sql.hive.plan.spark.sql.connector.hudi.HoodieMultiCatalogExtension
import org.apache.spark.sql.hive.plan.spark.sql.execution.IcebergStrategy
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ResolveCatalogViews
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser
import org.apache.spark.sql.hive.plan.{CLSSecRule, CustomDataSourceAnalyzer, CustomOptimizedPlan, CustomStrategy, DescribeUnResolvedRelation, DescribeViewRelationRule, ExternalCatalogWrite, RowLevelFilter, TwoToThreePartRule}

class CustomExtensionSuite extends DeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {

    super.apply(extensions)

    extensions.injectParser { (session, parser) =>
     // val delegate = new DeltaSqlParser(parser)
     // new CustomParser(delegate)

      CustomSparkSQLParser
    }
    (new HoodieMultiCatalogExtension().apply(extensions))
    extensions.injectResolutionRule(session => new ResolveCatalogViews(session))
    extensions.injectOptimizerRule(session => new TwoToThreePartRule(session))
    extensions.injectResolutionRule(session => new ResolveProcedures(session))
    extensions.injectResolutionRule(session => new DescribeUnResolvedRelation(session))
    extensions.injectResolutionRule(session => new DescribeViewRelationRule(session))
   // extensions.injectResolutionRule(session => new RowLevelFilter(session))
    extensions.injectPostHocResolutionRule(session => new CustomDataSourceAnalyzer(session) )
    extensions.injectOptimizerRule(session=> new ExternalCatalogCutAnalyzer(session))

    // extensions.injectResolutionRule(session => new CLSSecRule(session) )

    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }

    extensions.injectOptimizerRule(CustomOptimizedPlan)

    extensions.injectOptimizerRule(_ => ReplaceStaticInvoke)
    extensions.injectOptimizerRule(ExternalCatalogWrite)
    extensions.injectPlannerStrategy(_ => CustomStrategy)
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
    extensions.injectPlannerStrategy { spark => IcebergStrategy(spark) }
    extensions.injectFunction(CustomAdd.fd)
    extensions.injectFunction(Fibo.fd)
    extensions.injectFunction(ModelFunc.fd)
    extensions.injectFunction(FiboIter.fd)
    extensions.injectFunction(FiboFuncIn.fd)
   // super.apply(extensions)



  }

}
