import io.delta.tables.hc.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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
    spark.sql("create view cat.cls_db2.v1(cls_id , name) as select *  from cat.cls_db2.ppt")
    spark.sql("describe formatted cat.cls_db2.v1").show()
  //  spark.sql("show columns in cat.cls_db2.v1").show()
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


  def icebergApp(spark:SparkSession):Unit={
    spark.sql("""create database if not exists cat.cls_db4""")

    spark.sql("create table cat.cls_db4.it(id int, name string) using iceberg PARTITIONED BY (name)")
    spark.sql("insert into cat.cls_db4.it values(1,'sh'), (2, 'su')")

    spark.sql(
      """
        |UPDATE cat.cls_db4.it
        |SET id = 3
        |WHERE name = 'sh';
        |""".stripMargin)
    println("**** first read after update ****")
    spark.read.table("cat.cls_db4.it").show()

    spark.sql(
      """
        |DELETE FROM cat.cls_db4.it
        |WHERE id = 3;
        |
        |""".stripMargin)


    println("**** second read after delete ****")
    spark.read.table("cat.cls_db4.it").show()



  }


  def deltaforNameApp(spark: SparkSession):Unit = {

    import spark.implicits._
    spark.sql("create database db")
    spark.sql("create table db.dt(id int, name string) using delta")
    spark.sql("insert into db.dt values (1, 'ss'), (2,'sh')")
//     var df3 = Seq(
//  (7, "John", 2.0),
//  (8, "Sunny", 3.0),
//  (9, "Xiaoyu", 4.0),
//  (10, "Shashi", 5.0),
//  (11, "Bharath", 6.0),
//  (12, "Vivek", 7.0)
//   ).toDF("col1", "col2", "col3")
//    spark.sql("create database default")

   // df3.write.partitionBy("col2").format("delta").mode(SaveMode.Overwrite).saveAsTable("dt")

    val dt = io.delta.tables.hc.DeltaTable.getDeltaTable(spark, "db.dt", "/tmp/dt")

   // dt.delete("id = 1")

  //  dt.toDF.show()

  //  spark.sql("select * from db.dt").show()

    val sourceDf = createMergeSourceDF(spark)
    dt.as("target").merge(sourceDf.as("source"),
      "target.id = source.id").whenMatched()
      .updateExpr(Map(
        "name" -> "source.name"
      )).execute()

    spark.sql("select * from db.dt").show()

  }

  def createMergeSourceDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)

    ))

    val data = Seq(
      Row(2, "Bob Updated"), // matched row -> update
      Row(4, "David") // new row -> insert
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

}
