package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StructType}

case class CustomAdd(name: String, left: Expression, right: Expression) extends BinaryExpression with ImplicitCastInputTypes {

  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    dataType match {
      case DecimalType.Fixed(precision, scale) => TypeUtils.getNumeric(dataType).plus(input1, input2)
      case LongType => input1.asInstanceOf[Long] + input2.asInstanceOf[Long]
      case IntegerType => input1.asInstanceOf[Int] + input2.asInstanceOf[Int]
      case s:StructType =>
        val field1 = input1.asInstanceOf[StructType].fields.filter(f => f.name.equalsIgnoreCase(name)).head
        val field2 = input2.asInstanceOf[StructType].fields.filter(f => f.name.equalsIgnoreCase(name)).head
      val ans = field1.dataType match {
        case DecimalType.Fixed(precision, scale) => TypeUtils.getNumeric(dataType).plus(field1, field2)
        case LongType => field1.asInstanceOf[Long] + field2.asInstanceOf[Long]
        case IntegerType => field1.asInstanceOf[Int] + field2.asInstanceOf[Int]
        case _ => throw new UnsupportedOperationException("not supported")
      }
      input1.asInstanceOf[StructType]
      case _ =>   throw new UnsupportedOperationException("not supported")

    }
  }

  def symbol = "+"

  override  def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    dataType match {
      case LongType | IntegerType | DoubleType | FloatType | ShortType =>

        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} = $eval1 $symbol $eval2;
           """.stripMargin
        })

      case s:StructType => nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
           |${ev.value} = $eval1 $symbol $eval2;
         """.stripMargin
      })
    }


  }

  override def dataType: DataType = left.dataType

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(left.dataType, right.dataType)
}


object CustomAdd{
  val fd: FunctionDescription = (
    new FunctionIdentifier("fadd"),
    new ExpressionInfo(classOf[CustomAdd].getCanonicalName, "fadd"),
    (children: Seq[Expression]) => CustomAdd(children.head.toString(),children(1), children.last)
  )
}


