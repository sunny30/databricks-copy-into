package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, CreateNamedStruct, Expression, ExpressionInfo, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StringType, StructType}
import org.apache.spark.sql.functions._

case class CustomAdd(name: String, left: Expression, right: Expression) extends BinaryExpression with ImplicitCastInputTypes {

  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    left.dataType match {
      case DecimalType.Fixed(precision, scale) => TypeUtils.getNumeric(dataType).plus(input1, input2)
      case LongType => input1.asInstanceOf[Long] + input2.asInstanceOf[Long]
      case IntegerType => input1.asInstanceOf[Int] + input2.asInstanceOf[Int]
      case s:StructType =>
        val field1 = left.asInstanceOf[CreateNamedStruct]
        val field2 = right.asInstanceOf[CreateNamedStruct]
        var structMap1 = ( field1.nameExprs.map(e => e.toString())  zip field1.valExprs).toMap
        val structMap2 = ( field2.nameExprs.map(e => e.toString())  zip field2.valExprs).toMap
        var f1 = structMap1(name)
        val f2 = structMap2(name)

      val ans = f1.dataType match {
        case DecimalType.Fixed(precision, scale) => TypeUtils.getNumeric(dataType).plus(f1, f2)
        case LongType => f1.asInstanceOf[Long] + f2.asInstanceOf[Long]
        case IntegerType => f1.asInstanceOf[Literal].value.asInstanceOf[Int] + f2.asInstanceOf[Literal].value.asInstanceOf[Int]
        case _ => throw new UnsupportedOperationException("not supported")
      }
        structMap1 = structMap1 + (name->lit(ans).expr)
        val expressions = structMap1.map(kv => Seq(lit(kv._1).expr, kv._2)).flatten.toList
        val result = CreateNamedStruct(expressions)
        printJson(result)
      case _ =>   throw new UnsupportedOperationException("not supported")

    }
  }

  override def prettyName: String = "fadd"

  def printJson(result: CreateNamedStruct): String = {
    var structMap1 = ( result.nameExprs.map(e => e.toString())  zip result.valExprs).toMap
    structMap1.mkString(",")
  }

  def symbol = "+"

  final override  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    dataType match {
      case IntegerType | ShortType =>

        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} =  java.lang.Integer.sum($eval1, $eval2);
           """.stripMargin
        })

      case LongType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} = java.lang.Long.sum($eval1, $eval2);
           """.stripMargin
        })

      case DoubleType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} = java.lang.Double.sum($eval1, $eval2);
           """.stripMargin
        })

      case FloatType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} = java.lang.Float.sum($eval1, $eval2);
           """.stripMargin
        })

      case s:StructType => nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
           |${ev.value} = $eval1 $symbol $eval2;
         """.stripMargin
      })
    }


  }

  override def dataType: DataType = left.dataType match {
    case s:StructType => StringType
    case _ => left.dataType
  }

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


