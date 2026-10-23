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
  def main(args: Array[String]):Unit={
    println(simpleClassEvalDemo(5))
  }

}
