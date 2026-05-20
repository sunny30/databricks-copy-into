import org.apache.spark.sql.SparkSession

object CLSApp {


  def viewCLSApp(spark: SparkSession):Unit = {

    spark.sql("""create database if not exists cat.cls_db2""")
    spark.sql("create table cat.cls_db2.ppt(id int, name string) using parquet PARTITIONED BY (name)")
   // spark.sql("describe table cat.cls_db2.ppt cls_id").show()
    //spark.sql("show columns in cat.cls_db2.ppt").show()
    spark.sql("insert into cat.cls_db2.ppt values(1,'sh'), (3, 'su')")
//    val cteQuery =
//      s"""WITH cte_data AS (SELECT * FROM cat.cls_db2.ppt)
//         |SELECT * FROM cte_data""".stripMargin
//    spark.sql(cteQuery).show()
   // spark.sql("SELECT * FROM cte_data").show()
   // spark.sql("truncate table cat.cls_db2.ppt")
   // spark.sql("select * from cat.cls_db2.ppt").show()

//    spark.sql(
//      """
//        |MERGE INTO cat.cls_db2.ppt AS target
//        |USING (
//        |SELECT * FROM VALUES
//        |    (1, 'Alice'),
//        |    (2, 'Bob')
//        |) AS source (id, name)
//        |ON target.id = source.id
//        |WHEN MATCHED THEN
//        |  UPDATE SET *
//        |WHEN NOT MATCHED THEN
//        |  INSERT *
//        |""".stripMargin)

  //  working merge
//    spark.sql(
//      """
//        |MERGE INTO cat.cls_db2.ppt AS target
//        |USING (SELECT 1 AS id, 'Alice' AS name, 100 AS amount) AS source
//        |ON target.id = source.id
//        |WHEN MATCHED THEN
//        |    UPDATE SET target.name = 'Alice'
//        |WHEN NOT MATCHED THEN
//        |    INSERT (id, name) VALUES (1, 'Alice');
//        |""".stripMargin)
   // spark.sql("update cat.cls_db2.ppt set id = 4 where name1 = 'sh'")
  //  spark.sql("select * from cat.cls_db2.ppt").show()
    spark.sql("create view cat.cls_db2.v1(id , cls_name) as select *  from cat.cls_db2.ppt")
    spark.sql("describe formatted cat.cls_db2.v1").show()
    spark.sql("show columns in cat.cls_db2.v1").show()
//    spark.sql("select * from cat.cls_db2.v1").show()
//    spark.sql("show columns in cat.cls_db2.ppt").show()
//    spark.sql("show columns in cat.cls_db2.v1").show()





  }

  def normalPathApp(spark: SparkSession):Unit ={
    spark.read.format("csv").option("inferSchema","true").option("header", "true").load("/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data").show()
  }

  def dataframeWriteOperation(spark: SparkSession):Unit ={
    spark.sql("""create database if not exists cat.cls_db3""")
    val df =  spark.read.format("csv").option("inferSchema","true").option("header", "true").load("/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data")
    df.write.format("parquet").saveAsTable("cat.cls_db3.ptbl")
    df.write.format("delta").saveAsTable("cat.cls_db3.dtbl")
    df.write.format("iceberg").saveAsTable("cat.cls_db3.itbl")

  }

}
