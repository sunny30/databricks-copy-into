import org.apache.spark.sql.SparkSession

object DeltaApp {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("DeltaLakeExample")
      // Enable Delta Lake SQL commands (like MERGE, UPDATE, DELETE)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      // Register Delta as the default catalog for table management
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local[*]") // Use all available cores on your local machine
      .getOrCreate()


    import spark.implicits._
    val data = Seq(
      (101, "A", "2025-01-15"),
      (102, "B", "2025-01-20"),
      (103, "C", "2025-01-25")
    )

    val data1 = Seq(
      (102, "A", "2025-01-15"),
      (103, "B", "2025-01-20"),
      (104, "C", "2025-01-25")
    )

    // 2. Convert to DataFrame and name the columns
    val replaceData1 = data1.toDF("id", "status", "start_date")
    val replaceData = data.toDF("id", "status", "start_date")

    val df3 = Seq(
      (7, "John", 2.0),
      (8, "Sunny", 3.0),
      (9, "Xiaoyu", 4.0),
      (10, "Shashi", 5.0),
      (11, "Bharath", 6.0),
      (12, "Vivek", 7.0)
    ).toDF("col1", "col2", "col3")


    val df4 = Seq(
      (7, "Hardy", 2.0),
      (8, "vacum", 3.0),
      (9, "sherlock", 4.0),
      (10, "johny", 5.0),
      (11, "Yes", 6.0),
      (12, "papa", 7.0)
    ).toDF("col1", "col2", "col3")

    val df5 = Seq(
      (7, "Hardy"),
      (8, "vacum"),
      (9, "sherlock"),
      (10, "johny"),
      (11, "Yes"),
      (12, "papa")
    ).toDF("id", "name")


    replaceData1.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("tbl5")


    replaceData.write
          .format("delta")
          .mode("error")
         // .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
          .saveAsTable("tbl5")

    // 3. Perform the selective overwrite
//    replaceData.write
//      .format("delta")
//      .mode("overwrite")
//      .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
//      .saveAsTable("tbl2")

   // spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

//    replaceData.write
//      .option("overwriteSchema", "true")
//      .format("delta")
//      .mode("overwrite")
//      .partitionBy("status")
//
//      //  .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
//      .saveAsTable("tbl4")

    spark.sql("describe history tbl4").show()
    spark.sql("describe detail tbl4").show()


    val dfw2 = replaceData.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")


  }

}
