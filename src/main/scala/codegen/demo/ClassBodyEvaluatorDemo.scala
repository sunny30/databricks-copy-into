package codegen.demo

import org.codehaus.janino.util.AbstractTraverser
import org.codehaus.janino.{Access, ClassBodyEvaluator, Java, Parser, Scanner}

import java.io.StringReader
import scala.collection.mutable

object ClassBodyEvaluatorDemo {

  // ---- model for the call tree (NOT java.lang.reflect.Method - that name clash broke the original) -------

  /** One call site inside a method. target is None for foo(), Some("this") for this.foo(). */
  final case class Call(target: Option[String], methodName: String, line: Int) {
    def isLocal: Boolean = target.isEmpty || target.contains("this")
  }

  /** One declared method or constructor. */
  final class MethodNode(val name: String, val signature: String, val access: String, val line: Int) {
    val calls: mutable.ArrayBuffer[Call] = mutable.ArrayBuffer.empty[Call]
  }

  // ---- sources --------------------------------------------------------------------------------------------

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

  lazy val fib: String =
    """public int fib(int n) {
      |  if (n < 0) return 0;
      |  if (n == 0 || n == 1) return 1;
      |  return fib(n - 1) + fib(n - 2);
      |}
      |""".stripMargin

  // ---- 1. call one known method -----------------------------------------------------------------------

  def simpleClassEvalDemo(n: Int): Int = {
    val cbe = new ClassBodyEvaluator()
    cbe.setParentClassLoader(ClassBodyEvaluatorDemo.getClass.getClassLoader)
    cbe.cook(fib)

    val clazz    = cbe.getClazz
    val instance = clazz.getDeclaredConstructor().newInstance()
    val method   = clazz.getMethod("fib", classOf[Int])
    method.invoke(instance, Int.box(n)).asInstanceOf[Int]
  }

  // ---- 2. call a function string with values by parameter NAME --------------------------------------

  /**
   * Reflection cannot give the names: Janino-compiled methods report arg0, arg1 even with debug information
   * (that needs javac -parameters). So read the names from the source with the parser, then compile.
   */
  def runFunctionWithDynamicEval(fn: String, values: Map[String, AnyRef]): AnyRef = {
    val decl       = firstMethod(fn)                                          // parse only
    val paramNames = decl.formalParameters.parameters.map(_.name)             // Array("n")

    val cbe = new ClassBodyEvaluator()
    cbe.setParentClassLoader(ClassBodyEvaluatorDemo.getClass.getClassLoader)
    cbe.cook(fn)

    // pick the method by name (getDeclaredMethods has no guaranteed order)
    val m = cbe.getClazz.getDeclaredMethods.find(_.getName == decl.name)
      .getOrElse(throw new IllegalStateException(s"method ${decl.name} not found"))

    val args = paramNames.map { n =>
      values.getOrElse(n, throw new IllegalArgumentException(
        s"no value for parameter '$n' (parameters: ${paramNames.mkString(", ")})"))
    }
    val target = if (java.lang.reflect.Modifier.isStatic(m.getModifiers)) null
    else cbe.getClazz.getDeclaredConstructor().newInstance()
    m.invoke(target, args: _*)
  }

  /** The first method declared in a class-body string. */
  private def firstMethod(classBody: String): Java.MethodDeclarator = {
    val cu = parseUnit("class Wrapper {\n" + classBody + "\n}").asInstanceOf[Java.CompilationUnit]
    cu.getPackageMemberTypeDeclarations()(0).asInstanceOf[Java.PackageMemberClassDeclaration]
      .getMethodDeclarations.get(0)
  }

  // ---- 3. call tree --------------------------------------------------------------------------------------

  /** Accepts a full class or just methods. No pointless try/catch/rethrow, no double wrapping. */
  def parseNestedFunctionCalls(src: String): Java.AbstractCompilationUnit =
    try parseUnit(src)                                                        // a complete class: parse as is
    catch { case _: Exception => parseUnit("class Wrapper {\n" + src + "\n}") } // just methods: wrap them

  private def parseUnit(src: String): Java.AbstractCompilationUnit =
    new Parser(new Scanner(null, new StringReader(src))).parseAbstractCompilationUnit()

  private def accessName(a: Access): String =
    if (a == Access.DEFAULT) "package" else a.toString.toLowerCase

  private def collect(cu: Java.AbstractCompilationUnit): mutable.LinkedHashMap[String, MethodNode] = {
    val methods = mutable.LinkedHashMap.empty[String, MethodNode]

    new AbstractTraverser[RuntimeException] {
      // The methods we are inside, innermost first. A List works on Scala 2.12, 2.13 and 3
      // (mutable.Stack is deprecated in 2.12).
      private var current: List[MethodNode] = Nil

      override def traverseMethodDeclarator(md: Java.MethodDeclarator): Unit = {
        enter(md, md.name, md.getAccess)
        super.traverseMethodDeclarator(md)
        current = current.tail                     // leave the method
      }

      override def traverseConstructorDeclarator(cd: Java.ConstructorDeclarator): Unit = {
        enter(cd, "<init>", cd.getAccess)
        super.traverseConstructorDeclarator(cd)
        current = current.tail
      }

      private def enter(fd: Java.FunctionDeclarator, name: String, access: Access): Unit = {
        val params = fd.formalParameters.parameters.map(_.`type`.toString).mkString(", ")
        val m = methods.getOrElseUpdate(name,
          new MethodNode(name, s"$name($params)", accessName(access), fd.getLocation.getLineNumber))
        current = m :: current                     // enter the method
      }

      override def traverseMethodInvocation(mi: Java.MethodInvocation): Unit = {
        current.headOption.foreach { m =>
          m.calls += Call(Option(mi.target).map(_.toString), mi.methodName, mi.getLocation.getLineNumber)
        }
        super.traverseMethodInvocation(mi)
      }

      // Janino 3.1.9's traverser skips variable initializers ("int y = f(x);"); dispatch them ourselves.
      override def traverseArrayInitializerOrRvalue(aiorv: Java.ArrayInitializerOrRvalue): Unit = aiorv match {
        case r: Java.Rvalue => this.visitAtom(r)
        case _              => super.traverseArrayInitializerOrRvalue(aiorv)
      }
    }.visitAbstractCompilationUnit(cu)

    methods
  }

  /** Entry points: non-private methods, plus private methods no OTHER method calls. */
  private def roots(methods: collection.Map[String, MethodNode]): Seq[String] = {
    val called = methods.values.flatMap { m =>
      m.calls.filter(c => c.isLocal && methods.contains(c.methodName) && c.methodName != m.name).map(_.methodName)
    }.toSet
    methods.values.filter(m => m.access != "private" || !called.contains(m.name)).map(_.name).toSeq
  }

  def render(methods: collection.Map[String, MethodNode], showExternal: Boolean = false): String = {
    val sb = new StringBuilder
    for (root <- roots(methods)) {
      val m = methods(root)
      sb.append(s"${m.signature}  [${m.access}, line ${m.line}]\n")
      children(methods, m, "", List(root), showExternal, sb)
      sb.append('\n')
    }
    sb.toString
  }

  private def children(methods: collection.Map[String, MethodNode], m: MethodNode, prefix: String,
                       path: List[String], showExternal: Boolean, sb: StringBuilder): Unit = {
    val shown = m.calls.filter(c => (c.isLocal && methods.contains(c.methodName)) || showExternal)
    shown.zipWithIndex.foreach { case (c, i) =>
      val last = i == shown.size - 1
      sb.append(prefix).append(if (last) "└── " else "├── ")
      (if (c.isLocal) methods.get(c.methodName) else None) match {
        case None =>
          sb.append(c.target.map(_ + ".").getOrElse("") + c.methodName + s"  (external, line ${c.line})\n")
        case Some(cm) if path.contains(cm.name) =>
          sb.append(s"${cm.signature}  (recursive, line ${c.line})\n")
        case Some(cm) =>
          sb.append(s"${cm.signature}  [${cm.access}, called at line ${c.line}]\n")
          children(methods, cm, prefix + (if (last) "    " else "│   "), cm.name :: path, showExternal, sb)
      }
    }
  }

  // ---- run all three ------------------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    println("fib(10) via reflection = " + simpleClassEvalDemo(10))

    val nv: Int = 6
    println("fib(6) via names       = " + runFunctionWithDynamicEval(fib, Map("n" -> Int.box(nv))))

    val cu = parseNestedFunctionCalls(source)
    println()
    println(render(collect(cu)))
  }
}