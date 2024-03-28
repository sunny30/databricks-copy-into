package org.apache.spark.sql.hive.util

object App {

  def main(args:Array[String]):Unit={
    import ExtendObj._
    val classicalClass = new ClassicalClass(1,"sharad")
    println(classicalClass.concatWithOne)
  }
}
