package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType, StructType}

case class FiboFuncIn(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any): Any = {
    println("eval is invoked")
    child.dataType match {
      case IntegerType =>
        val n = input1.asInstanceOf[Int]
        Fibo.evalFibo(n)
      case _ => throw new IllegalArgumentException("only integers are allowed in this expression")

    }
  }

  override def prettyName: String = "fiboinline"

  override def nullable: Boolean = false


  override def dataType: DataType = child.dataType match {
    case s: StructType => StringType
    case _ => child.dataType
  }


  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(child.dataType)


  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case IntegerType =>
        nullSafeCodeGen(ctx, ev, c => {
          val fiboFunc = ctx.freshName("fiboFunc")

          val funcCode =
            s"""
              |int ${fiboFunc}(int n) {
              |    if(n<0){
              |      return 0 ;
              |    }
              |    if(n==0 || n==1)
              |      return 1 ;
              |   else{
              |      return ${fiboFunc}(n-1)+${fiboFunc}(n-2) ;
              |    }
              |  }
              |""".stripMargin

          val sysFuncName = ctx.addNewFunction(fiboFunc, funcCode)
          s"""
            |${ev.value} = ${fiboFunc}($c) ;
            |""".stripMargin

        })
    }
  }

}


object FiboFuncIn{
  val fd: FunctionDescription = (
    new FunctionIdentifier("fiboinline"),
    new ExpressionInfo(classOf[FiboFuncIn].getCanonicalName, "fiboinline"),
    (children: Seq[Expression]) => FiboFuncIn(children.head)
  )
}
