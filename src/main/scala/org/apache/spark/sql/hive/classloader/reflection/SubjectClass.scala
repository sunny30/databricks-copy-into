package org.apache.spark.sql.hive.classloader.reflection

class SubjectClass(name: String, value:Int) {

  def printValues(anotherName:String, anotherValue:Int): Unit = {
    println(s"${anotherName} has value ${anotherValue.toString}")
  }

}

