package codegen.demo

import org.apache.commons.lang.ClassUtils
import org.codehaus.janino.{ExpressionEvaluator, Java, Parser, Scanner, TokenType}

import java.io.StringReader

object ExpressionEvaluatorDemo {

  case class SimpleExpression(evalJavaExpression:String){
    def parameterNames: Array[String] = {
      ExpressionEvaluator.guessParameterNames(new Scanner(null, new StringReader(evalJavaExpression)))
    }

    def getTypeParameter(dataType: String): Array[Class[_]] = {
      val c: Class[_] = ClassUtils.getClass(dataType)
      Array.fill[Class[_]](parameterNames.length)(c)
    }
  }

  def parseJavaExpression(javaExpression: String):Java.Rvalue={
    val p = new Parser(new Scanner(null, new StringReader(javaExpression)))
    val r = p.parseExpression()
    if (!p.peek(TokenType.END_OF_INPUT))
      throw new IllegalArgumentException(s"unexpected '${p.peek()}' after expression")
    r
  }

  def evaluateDynamicExpression(arithmeticExpr: String, values: Array[AnyRef]):Unit = {
    val simpleExpression = SimpleExpression(arithmeticExpr)
    val parameterNames = simpleExpression.parameterNames
    val typeInput = simpleExpression.getTypeParameter("int")
    val ee = new ExpressionEvaluator()
    ee.setParameters(parameterNames,typeInput)
    ee.setExpressionType(typeInput.head)
    ee.cook(arithmeticExpr)
    println(ee.evaluate(values: _*))

  }

  def evaluateSimpleExpression(predicate:String):Unit={
    val ee = new ExpressionEvaluator()
    ee.setParameters(Array("a", "b"), Array[Class[_]](classOf[Int], classOf[Int]))
    ee.setExpressionType(classOf[Int])
    ee.cook("a*b+10")
    val result: AnyRef = ee.evaluate(Array[AnyRef](Int.box(6), Int.box(7)): _*)
    println(result.asInstanceOf[Int])
  }
  def main(array: Array[String]):Unit={
   // evaluateSimpleExpression("a*b+10")

    evaluateDynamicExpression("a*b+10*c", Array[AnyRef](Int.box(6), Int.box(7), Int.box(5)))
    parseJavaExpression("a && (b || (c && d))")

  }

}
