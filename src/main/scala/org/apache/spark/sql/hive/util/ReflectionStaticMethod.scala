package org.apache.spark.sql.hive.util

object ReflectionStaticMethod {

  def main(args: Array[String]): Unit = {
    staticMethodInvocUtil
  }

  def staticMethodInvocUtil: Unit ={
    val clazz = Class.forName("org.apache.spark.sql.hive.util.TestClass")
    val s = clazz.getDeclaredMethod("getType").invoke(null)
    println(s)

  }

}
