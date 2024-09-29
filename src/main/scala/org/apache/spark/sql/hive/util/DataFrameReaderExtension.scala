package org.apache.spark.sql.hive.util

import org.apache.spark.sql.{DataFrame, DataFrameReader}

object DataFrameReaderExtension {

  implicit class DataFrameReaderExtension(val reader: DataFrameReader) {

    def extraOptions = {
      val m = reader.getClass.getDeclaredField("extraOptions")
      m.setAccessible(true)
      m.get(reader).asInstanceOf[Map[String, String]]


    }


    def model(name: String, inputDf: DataFrame): DataFrame = {
      println("hey inside model")

      println(extraOptions.mkString(":"))
      inputDf
    }

  }
}
