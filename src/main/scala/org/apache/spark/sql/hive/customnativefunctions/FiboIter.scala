package org.apache.spark.sql.hive.customnativefunctions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType, StructType}

case class FiboIter(child:Expression) extends UnaryExpression with ImplicitCastInputTypes{


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

  override def prettyName: String = "fiboiter"

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
          nullSafeCodeGen(ctx, ev, c =>{
            val prev1 = ctx.freshName("prev1")
            val prev2 = ctx.freshName("prev2")
            val i = ctx.freshName("i")
            val curr = ctx.freshName("curr")
            s"""
              |int ${prev1} = 1 ;
              |int ${prev2} = 1 ;
              |int ${curr} = 1 ;
              | if($c == 0 || $c == 1 ){
              |   ${ev.value} = 1 ;
              | }else {
              |   for (int $i = 2; $i <= $c; $i++){
              |     $curr = $prev1 + $prev2 ;
              |     $prev1 = $prev2 ;
              |     $prev2 = $curr ;
              |   }
              |   ${ev.value} = $curr ;
              |}
              |""".stripMargin
          })
      }
  }
}


object FiboIter{
  val fd: FunctionDescription = (
    new FunctionIdentifier("fiboiter"),
    new ExpressionInfo(classOf[FiboIter].getCanonicalName, "fiboiter"),
    (children: Seq[Expression]) => FiboIter(children.head)
  )
}




