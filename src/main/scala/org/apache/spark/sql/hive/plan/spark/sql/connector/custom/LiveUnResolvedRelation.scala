package org.apache.spark.sql.hive.plan.spark.sql.connector.custom

import org.apache.spark.sql.catalyst.expressions.Expression

case class LiveUnResolvedRelation(expressions:Seq[Expression])


