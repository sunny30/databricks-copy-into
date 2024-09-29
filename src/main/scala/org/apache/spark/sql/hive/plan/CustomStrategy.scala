package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object CustomStrategy extends Strategy with Serializable  {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    plan match {
      case _ => Nil
    }

}
