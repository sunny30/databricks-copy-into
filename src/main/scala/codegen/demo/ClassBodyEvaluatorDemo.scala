package codegen.demo

import org.codehaus.janino.ClassBodyEvaluator

object ClassBodyEvaluatorDemo {

  lazy val fib =
    """
      |public int fib(int n){
      | if(n<0)
      |   return 0 ;
      | if(n == 0 || n == 1)
      |   return 1 ;
      | else{
      |   return fib(n-1) + fib(n-2) ;
      |   }
      |}
      |""".stripMargin


  def simpleClassEvalDemo(n:Int):Int = {
    val cbe = new ClassBodyEvaluator()
    cbe.setParentClassLoader(ClassBodyEvaluatorDemo.getClass.getClassLoader)
    cbe.cook(fib)

    val clazz = cbe.getClazz // the generated class
    val instance = clazz.getDeclaredConstructor().newInstance() // fib is not static: need an object
    val method = clazz.getMethod("fib", classOf[Int]) // find fib(int)
    method.invoke(instance, Int.box(n)).asInstanceOf[Int]
  }


  def runFunctionWithDynamicEval(fn: String, values: Map[String, AnyRef]): AnyRef = {
    val cbe = new ClassBodyEvaluator()
    cbe.cook(fn)
    val m = cbe.getClazz.getDeclaredMethods.head
    val args = m.getParameters.map(p => values(p.getName)) // needs debug info, see below
    val obj = cbe.getClazz.getDeclaredConstructor().newInstance()
    val result = m.invoke(obj, args: _*)
    println(result)
    result
  }
  def main(args: Array[String]):Unit={
    println(simpleClassEvalDemo(5))
    val nv: Int = 6
    runFunctionWithDynamicEval(fib, Map("arg0"->(Int.box(nv))))
  }

}
