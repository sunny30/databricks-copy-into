package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

package object extra {

  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

}
