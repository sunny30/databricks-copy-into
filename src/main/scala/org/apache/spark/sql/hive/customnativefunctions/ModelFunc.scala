package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.hive.classloader.reflection.ReflectionUtil
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType, StructType}
import org.apache.spark.unsafe.types
import org.apache.spark.unsafe.types.UTF8String

case class ModelFunc(name: String, child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any):Any={

    child.dataType match {
      case StringType => val inputAsString = input1.toString
      ModelFunc.evalString(name,inputAsString)

      case s:StructType => ModelFunc.evalStruct(name,s)

      case _ => throw new IllegalArgumentException("not supported data type")
    }

  }

  override def prettyName: String = "query_model"

  override def nullable: Boolean = false


  override def dataType: DataType = StringType

  final override  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ={
    val clazz = ModelFunc.getClass.getCanonicalName.stripSuffix("$")
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, str => s"""$clazz.evalStringCodegen("$name",$str)""")
      case s: StructType => defineCodeGen(ctx, ev, str => s"""$clazz.evalStruct("$name", $str)""")
      case _ => throw new IllegalArgumentException("not supported data type")
    }


  }


  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(child.dataType)
}

object ModelFunc{

  def evalString(modelName: String, input:String):UTF8String={
    UTF8String.fromString(ReflectionUtil.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText",modelName, input))
   // input
  }

  def evalStringCodegen(modelName: String, input: UTF8String): UTF8String = {
    UTF8String.fromString(ReflectionUtil.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText", modelName, input.toString))
    // input
  }

  def evalStruct(modelName: String, input: StructType):UTF8String = {
    UTF8String.fromString(input.prettyJson)

  }

  val fd: FunctionDescription = (
    new FunctionIdentifier("query_model"),
    new ExpressionInfo(classOf[ModelFunc].getCanonicalName, "query_model"),
    (children: Seq[Expression]) => ModelFunc(children.head.toString(),children(1)))
}
