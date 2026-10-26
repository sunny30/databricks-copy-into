package codegen.demo

import org.codehaus.janino.{ClassBodyEvaluator, Java, Parser, Scanner}

import java.io.StringReader

object ClassBodyEvaluatorDemo {

  val source: String =
    """public class OrderService {
      |  public OrderService() { init(); }
      |  public double process(String id, double amount) {
      |    validate(id);
      |    double taxed = applyTax(amount);
      |    log("processed " + id);
      |    return round(taxed);
      |  }
      |  public void refund(String id) { validate(id); log("refund " + id); }
      |  private void init() { log("init"); }
      |  private void validate(String id) {
      |    if (id == null || id.isEmpty()) throw new IllegalArgumentException("id");
      |    checkFormat(id);
      |  }
      |  private void checkFormat(String id) { if (!id.startsWith("ORD")) log("bad id"); }
      |  private double applyTax(double a) { return a * rate(); }
      |  private double rate() { return 0.18 + 1; }
      |  private double round(double v) { return Math.round(v * 100) / 100.0; }
      |  private void log(String m) { System.out.println(m); }
      |  private int depth(int n) { return n <= 0 ? 0 : 1 + depth(n - 1); }
      |}
      |""".stripMargin

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

  def parseNestedFunctionCalls():Java.AbstractCompilationUnit = {
    //val cu = Java.AbstractCompilationUnit
    try {
      val wrapperSource =
        s"""class Wrapper{
          | ${source}
          |}""".stripMargin
      val cu = new Parser(new Scanner(null, new StringReader(wrapperSource))).parseAbstractCompilationUnit
      cu
    }catch {
      case e:Exception => throw e
    }
  }

  def main(args: Array[String]):Unit={
//    println(simpleClassEvalDemo(5))
//    val nv: Int = 6
//    runFunctionWithDynamicEval(fib, Map("arg0"->(Int.box(nv))))
    parseNestedFunctionCalls()

  }

}
