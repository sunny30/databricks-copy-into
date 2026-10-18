package codegen.demo

import org.codehaus.janino.ExpressionEvaluator

object ExpressionEvaluatorDemo {

  def evaluateSimpleExpression(predicate:String):Unit={
    val ee = new ExpressionEvaluator()
    ee.setParameters(Array("a", "b"), Array[Class[_]](classOf[Int], classOf[Int]))
    ee.setExpressionType(classOf[Int])
    ee.cook("a*b+10")
    val result: AnyRef = ee.evaluate(Array[AnyRef](Int.box(6), Int.box(7)): _*)
    println(result.asInstanceOf[Int])
  }
  def main(array: Array[String]):Unit={
    evaluateSimpleExpression("a*b+10")

  }

}
