package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType, StructType}

case class Fibo(child:Expression) extends UnaryExpression with ImplicitCastInputTypes{

  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any):Any={

    child.dataType match {
      case IntegerType =>
        val n = input1.asInstanceOf[Int]
        Fibo.evalFibo(n)
      case _ => throw new IllegalArgumentException("only integers are allowed in this expression")

    }
  }

  override def prettyName: String = "fibo"

  override def nullable: Boolean = false

  final override  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ={

    val hex = Fibo.getClass.getCanonicalName.stripSuffix("$")
    defineCodeGen(ctx, ev, str => s"$hex.evalFibo($str)")

  }

  override def dataType: DataType = child.dataType match {
    case s: StructType => StringType
    case _ => child.dataType
  }


  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(child.dataType)
}

object Fibo{
  def evalFibo(n:Int):Int = {
    if(n<0){
      return 0
    }
    if(n==0 || n==1)
      1
   else{
      return evalFibo(n-1)+evalFibo(n-2)
    }
  }

  val fd: FunctionDescription = (
    new FunctionIdentifier("fibo"),
    new ExpressionInfo(classOf[Fibo].getCanonicalName, "fibo"),
    (children: Seq[Expression]) => Fibo(children.head))
}
