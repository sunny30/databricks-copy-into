package org.apache.spark.sql.hive.classloader.reflection

import scala.reflect.runtime.{universe => ru}
import ru._
import scala.reflect.internal.util.ScalaClassLoader

object ReflectionUtil {

  def reflect(clazzName: String, methodName: String): Unit = {

    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = m.staticClass(clazzName)
    val ctor = classSymbol.primaryConstructor.asMethod
    val method = classSymbol.toType.decl(ru.TermName(methodName)).asMethod
    val cm = m.reflectClass(classSymbol)
    val instance =  cm.reflectConstructor(ctor).apply("param1",2)

  //  val instance = ctorm(constructorParams: _*)
    val instancem = m.reflect(instance)
    val methodm = instancem.reflectMethod(method)
    methodm.apply("maparam1", 212)

  }

  def reflectThis(clazzName: String, methodName: String,modelName:String,input:String): String = {

    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = m.staticClass(clazzName)
    val ctor = classSymbol.primaryConstructor.asMethod
    val constructors = classSymbol.typeSignature.members.filter(_.isConstructor).toList
    val secCtor = constructors.filter(c => c.asMethod.paramLists(0).length==3).head
    constructors.foreach(c => println(c.asMethod.paramLists.length))
    val method = classSymbol.toType.decl(ru.TermName(methodName)).asMethod
 //   val thisMethod = classSymbol.toType.decl(ru.TermName("this")).asMethod
    val cm = m.reflectClass(classSymbol)

    val instance = cm.reflectConstructor(secCtor.asMethod).apply(modelName, 2, null)

    //  val instance = ctorm(constructorParams: _*)
    val instancem = m.reflect(instance)

 //   val thism = instancem.reflectMethod(thisMethod)
    val methodm = instancem.reflectMethod(method)
 //   thism.apply(modelName)
    val result = methodm.apply(input)
    println(result)
    result.toString

  }

}

object App{
  def main(args: Array[String]): Unit ={
    (new SubjectClass("param1",2)).printValues("mparam",212)
  //  ReflectionUtil.reflect("org.apache.spark.sql.hive.classloader.reflection.SubjectClass", "printValues")
    ReflectionUtil.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText",modelName = "model","ttext")
  }
}
