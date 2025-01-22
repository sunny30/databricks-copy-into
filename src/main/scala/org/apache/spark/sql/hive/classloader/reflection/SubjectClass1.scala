package org.apache.spark.sql.hive.classloader.reflection

class SubjectClass1 {


  var name: String = _ ;
  var value: Int = _ ;
  var extra: String = _ ;
  def this(name:String, value:Int, extra:String=null) {
    this()
    this.name = name
    this.value = value
    this.extra = extra


  }

  def inputText(input:String):String = {
    input
  }


}
