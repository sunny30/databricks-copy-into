package org.apache.spark.sql.hive.util
import org.apache.spark.sql.delta.actions.Format
object ExtendObj{
  implicit class ExtendClass(obj: ClassicalClass) {
    def concatWithOne:String={
        obj.concat+"_one"
    }

    val f = Format(
      provider = "orc",
    )
  }
}
