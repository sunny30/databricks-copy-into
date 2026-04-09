import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.hive.datashare.{ConverterUtil, FSUtils}
import org.apache.spark.sql.hive.plan.spark.sql.execution.DiscoverCatalogPartition
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.plan.listener.CatalogQueryExecutionListener
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser
import org.apache.spark.sql.hive.plan.spark.sql.stat.AnalyzeCommandUtil
import org.apache.spark.sql.types.DecimalType


object App {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]").
      set("spark.sql.hive.metastore.version", "3.1.3").
      set("spark.sql.hive.metastore.jars", "path").
      set("spark.sql.test.env", "true").
      set("spark.sql.hive.metastore.jars.path", "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/parquet-column-1.13.1.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-metastore-3.1.3.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/jackson-core-2.6.7.jar" +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/jackson-databind-2.6.7.3"+
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-exec-3.1.3.jar, " +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-logging-1.1.1.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-io-2.7.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/calcite-core-1.32.0.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/thrift-1.0.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/libfb303-0.9.3.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/dropwizard-core-2.1.5.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/metrics-core-3.0.2.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-common-3.1.3.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-core-4.1.17.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-rdbms-4.1.17.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-api-jdo-4.2.4.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/HikariCP-2.5.1.jar," +
        "/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/derby-10.14.2.0.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/javax.jdo-3.2.0-release.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-collections-3.2.2.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/antlr-runtime-3.5.3.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/servlet-api-2.3.jar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hive.CustomExtensionSuite")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hive.catalog.UnityCatalog")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("parquet.compression", "SNAPPY")
      .set("spark.sql.sources.default", "delta")
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
      .set("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .set("spark.sql.parquet.aggregatePushdown", "true")
      .set("spark.sql.sources.commitProtocolClass","org.apache.spark.sql.hive.plan.spark.sql.connector.manifest.ManifestFileCommitProtocolV2")
    //   .set("spark.sql.parquet.enableVectorizedReader","false")
    //   .set("parquet.strict.typing","false")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.5.1-lake").master("local").
      config(getConf).
      enableHiveSupport().
      getOrCreate()


    /** Custom data format  write options* */

    import spark.implicits._
    //spark.sql("create table db.t1(id int) using parquet")
  //  spark.sql("create database cat.hivedb")
    //spark.sql("CREATE TABLE cat.hivedb.student_text1 (id INT, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
   // spark.sql("create table cat.customdb.tbl(price int,greet string, id double ) using custom options('k'='v', 'k1' = 'v1')")
    // spark.sql("select * from cat.customdb.tbl").show()
    // spark.sql("select greet from cat.customdb.tbl").show()
    //spark.sql("insert into cat.customdb.tbl values(7, 'ss',2.0)")
    var df3 = Seq(
      (7, "John", 2.0),
      (8, "Sunny", 3.0),
      (9, "Xiaoyu", 4.0),
      (10, "Shashi", 5.0),
      (11, "Bharath", 6.0),
      (12, "Vivek", 7.0)
    ).toDF("col1", "col2", "col3")

   // spark.sql("""create table singlet03(id int) using parquet""")
    //location '/tmp/etbl'
    //spark.sql("create table in04(col1 int, col3 double, col2 string) using parquet partitioned by(col2) ")
   // df3 = df3.select("col1", "col3", "col2")
   // df3.write.insertInto("in04")
    //df3.write.partitionBy("col2").format("parquet").mode(SaveMode.Append).saveAsTable("int05")
//    df3.write.partitionBy("col2").format("parquet").mode(SaveMode.Append).saveAsTable("in80")
//    df3.write.mode(SaveMode.Append).saveAsTable("in80")
//    spark.read.table("in80").show()

    spark.sql("create table ht1(id int) using hive options('fileformat' = 'parquet')")


    //

    //df3.write.partitionBy("col2").option("path", "/tmp/etbl").format("parquet").mode(SaveMode.Overwrite).saveAsTable("in0")
   // spark.read.table("in0").show()

//    df3 = df3.select("col1", "col3", "col2")
//    df3.write.partitionBy("col2").format("delta").mode(SaveMode.Overwrite).saveAsTable("single04")

//    val data = Seq(
//      (101, "A", "2025-01-15"),
//      (102, "B", "2025-01-20"),
//      (103, "C", "2025-01-25")
//    )
//
//    val data1 = Seq(
//      (102, "A", "2025-01-15"),
//      (103, "B", "2025-01-20"),
//      (104, "C", "2025-01-25")
//    )

//    spark.sql("""create table singlet01(id int) using parquet""")
//    spark.sql("insert into singlet01 values(1), (2)")
//
//    spark.sql("select * from singlet01").show()

//   df3 = df3.select("col1", "col3", "col2")
//   df3.write.partitionBy("col2").format("parquet").mode("overwrite").save("/tmp/pt")
//   df3.filter("col1>9").write.partitionBy("col2").format("parquet").mode("overwrite").save("/tmp/pt")


//// //   spark.read.format("parquet").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/manifest_db.db/tbl/").show()
////
//    spark.sql("""create database if not exists cat.manifest_db""")
//    df3.write.option("path", "/tmp/mtbl").partitionBy("col2").format("parquet").mode("overwrite").saveAsTable("cat.manifest_db.tbl")
//    df3.filter("col1 > 9").write.partitionBy("col2").format("parquet").mode("overwrite").saveAsTable("cat.manifest_db.tbl")
////
//
//    df3.write.format("parquet").mode("overwrite").saveAsTable("cat.manifest_db.tbl1")
//    df3.write.format("parquet").mode("overwrite").saveAsTable("cat.manifest_db.tbl1")




//    val df2 = df3.select("col1", "col3", "col2")
//    /**write begins**/
//    spark.sql("""create database if not exists cat.my_database""")
//    df2.write
//      .format("parquet") // or "delta", "orc", etc.
//      .option("path", "/tmp/wrp/") // Sets the external data location
//      .partitionBy("col2") // Specifies partition columns
//      .mode("append") // Options: "overwrite", "append", "ignore", "error"
//      .saveAsTable("cat.my_database.my_table_name")
//
//    spark.sql("describe formatted cat.my_database.my_table_name").show()
//    spark.read.table("cat.my_database.my_table_name").show()
//
//    df2.write
//      .format("parquet") // or "delta", "orc", etc.
//      .option("path", "/tmp/wrp/") // Sets the external data location
//      .partitionBy("col2") // Specifies partition columns
//      .mode("append") // Options: "overwrite", "append", "ignore", "error"
//      .saveAsTable("cat.my_database.my_table_name")
//   // spark.sql("describe formatted cat.my_database.my_table_name").show()
//    spark.read.table("cat.my_database.my_table_name").show()
//
//
//    df2.write
//      .format("parquet") // or "delta", "orc", etc.
//      .option("path", "/tmp/wrp1/") // Sets the external data location
//     // .partitionBy("col2") // Specifies partition columns
//      .mode("append") // Options: "overwrite", "append", "ignore", "error"
//      .saveAsTable("cat.my_database.my_table_name1")
//    spark.read.table("cat.my_database.my_table_name1").show()
//
//
//    df2.write
//      .format("parquet") // or "delta", "orc", etc.
//     // .option("path", "/tmp/wrp1/") // Sets the external data location
//       .partitionBy("col2") // Specifies partition columns
//      .mode("append")
//      // Options: "overwrite", "append", "ignore", "error"
//      .saveAsTable("cat.my_database.my_table_name2")
//    spark.read.table("cat.my_database.my_table_name2").show()

//    df2.write
//      .format("parquet") // or "delta", "orc", etc.
//      // .option("path", "/tmp/wrp1/") // Sets the external data location
//    //  .partitionBy("col2")
//      // Specifies partition columns
//      .mode("append")
//      // Options: "overwrite", "append", "ignore", "error"
//      .saveAsTable("cat.my_database.my_table_name3")
//    spark.read.table("cat.my_database.my_table_name3").show()


    /***write ends***/




//    val df4 = Seq(
//      (7, "Hardy", 2.0),
//      (8, "vaccum", 3.0),
//      (9, "sherlock", 4.0),
//      (10, "johny", 5.0),
//      (11, "Yes", 6.0),
//      (12, "papa", 7.0)
//    ).toDF("col1", "col2", "col3")
//
//    val df5 = Seq(
//      (7, "Hardy"),
//      (8, "vacum"),
//      (9, "sherlock"),
//      (10, "johny"),
//      (11, "Yes"),
//      (12, "papa")
//    ).toDF("id", "name")

    /**delta convert and cdc changes for three part name**/

//    spark.sql("""create database if not exists cat.ddb1""")
//    spark.sql("create table cat.ddb1.dt(id int) using delta TBLPROPERTIES (delta.enableChangeDataFeed = true)")
//    spark.sql("insert into cat.ddb1.dt values (1), (2)")
//    spark.sql("insert into cat.ddb1.dt values (3), (4)")
//    spark.sql("insert into cat.ddb1.dt values (5), (6)")
//    spark.sql("SELECT * FROM table_changes('cat.ddb1.dt', 0, 2)").show()
   // println("SQL output of CDC")
//    spark.read.format("delta")
//      .option("readChangeFeed", "true")
//      .option("startingVersion", 0).table("cat.ddb1.dt").show()
//    println("Dataframe output of CDC")


//    spark.sql("""create database if not exists cat.ddb2""")
//    spark.sql("create table cat.ddb2.ppt(id int, name1 string) using parquet PARTITIONED BY (name1)")
//    spark.sql("create table cat.ddb2.pt(id int, name1 string) using parquet")
//    spark.sql("insert into cat.ddb2.ppt values(1,'sh'), (2, 'su')")
//    spark.sql("insert into cat.ddb2.pt values(1,'sh'), (2, 'su')")
//
//    spark.sql("CONVERT TO DELTA cat.ddb2.ppt")
//    spark.sql("CONVERT TO DELTA cat.ddb2.pt")
//    spark.sql("describe formatted cat.ddb2.ppt").show()
//    spark.sql("describe formatted cat.ddb2.pt").show()
//    spark.sql("describe history cat.ddb2.ppt").show()
//    spark.sql("describe history cat.ddb2.pt").show()
    /**ends with delta convert and cdc changes for three part name**/


//    spark.sql("""create database if not exists cat.ddb4""")
//    spark.sql("create table cat.ddb4.ppt(id int, name1 string) using parquet PARTITIONED BY (name1)")
//    spark.sql("insert into cat.ddb4.ppt values(1,'1'), (2, '2')")
//
//    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
//    spark.sql("select distinct name1 from cat.ddb4.ppt").show()

//    spark.sql("""create database if not exists cat.ddb3""")
//    spark.sql("create table cat.ddb3.ppt(id int, name1 string) using parquet PARTITIONED BY (name1)")
//    spark.sql("create table cat.ddb3.pt(id int, name1 string) using parquet")
//    spark.sql("insert into cat.ddb3.ppt values(1,'sh'), (2, 'su')")
//    spark.sql("insert into cat.ddb3.pt values(1,'sh'), (2, 'su')")


//    df5.createTempView("tmp_v")
//
//
//
//    //spark.sessionState.sqlParser.parsePlan("select * from cat.db.t, cat.db.t1")
//    spark.sql("""create database if not exists cat.idb4""")
//
//    //spark.sql("create table cat.idb4.tbl using parquet location '/tmp/tb' as select * from tmp_v")
//
////    spark.sql("create table cat.idb4.ptbl(cls_id int, age int) using parquet  PARTITIONED BY (age)")
////    spark.sql("insert into cat.idb4.ptbl values (1,30), (2,30), (3,31), (4,31)")
////
////  //  spark.read.table(" cat.idb4.ptbl").show()
////    spark.sql("select * from cat.idb4.ptbl").show()
////
////    spark.sql("create view cat.idb4.v1(id , cls_age) as select *  from cat.idb4.ptbl")
////    spark.sql("create view cat.idb4.v1(a,cls_b) as select cls_id, age from cat.idb4.ptbl")
////    spark.sql("describe formatted cat.idb4.v1").show()
////    spark.sql("select * from cat.idb4.v1").show()
//
//    spark.sql("create table cat.idb4.dtbl(cls_id int, age int) using delta  PARTITIONED BY (age)")
//    spark.sql("insert into cat.idb4.dtbl values (1,30), (2,30), (3,31), (4,31)")
//
//    //  spark.read.table(" cat.idb4.ptbl").show()
//    spark.sql("select * from cat.idb4.dtbl").show()
//
//    spark.sql("create view cat.idb4.v2(id , cls_age) as select *  from cat.idb4.dtbl")
//   // spark.sql("create view cat.idb4.v1(a,cls_b) as select cls_id, age from cat.idb4.ptbl")
//    spark.sql("describe formatted cat.idb4.v2").show()
//    spark.sql("select * from cat.idb4.v2").show()
//
//
//    spark.sql("create table cat.idb4.itbl(cls_id int, age int) using iceberg  PARTITIONED BY (age)")
//    spark.sql("insert into cat.idb4.itbl values (1,30), (2,30), (3,31), (4,31)")
//
//    //  spark.read.table(" cat.idb4.ptbl").show()
//    spark.sql("select * from cat.idb4.itbl").show()
//
//    spark.sql("create view cat.idb4.v3(id , cls_age) as select *  from cat.idb4.itbl")
//    // spark.sql("create view cat.idb4.v1(a,cls_b) as select cls_id, age from cat.idb4.ptbl")
//    spark.sql("describe formatted cat.idb4.v3").show()
//    spark.sql("select * from cat.idb4.v3").show()





    // DeltaTable.forName(spark,"").toDF
//    df5.write.mode(SaveMode.Overwrite).saveAsTable("cat.deltadb.tbl")
//
//    df4.write.mode(SaveMode.Overwrite).saveAsTable("cat.deltadb.tbl")
//
//    spark.read.table("cat.deltadb.tbl").show()

    import spark.implicits._

    // 1. Define your data as a Sequence of Tuples


    // 2. Convert to DataFrame and name the columns
  //  val replaceData1 = data1.toDF("id", "status", "start_date")
//    spark.sql("""create database if not exists cat.idb9""")
//    spark.sql("create table cat.idb9.tbl(id int) using delta")
//    replaceData1.write
//          .format("delta")
//          .mode("overwrite").option("overwriteSchema", "true")
//          .saveAsTable("cat.idb9.tbl")
//    spark.read.table("cat.idb9.tbl").show()
//
//    spark.sql("describe history cat.idb9.tbl")
//    val replaceData = data.toDF("id", "status", "start_date")
//
//    replaceData.writeTo("cat.deltadb2.dtbl1")
//      .using("delta")
//      .tableProperty("path", "/tmp/delta1")
//      .create()
//
//    spark.sql("select * from cat.deltadb2.dtbl1 where id = 101").show()
//
//
//    replaceData1.write
//      .format("delta")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.dtbl1")
//
//    spark.sql("select * from cat.deltadb1.dtbl1").show()
//
//    spark.sql("describe history cat.deltadb1.dtbl1").show()

//    replaceData1.writeTo("cat.deltadb1.itbl1")
//      .using("iceberg")
//      .tableProperty("path", "/tmp/ice")
//      .createOrReplace()
//
//    replaceData.write
//      .format("iceberg")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.itbl1")
//
//
//    spark.sql("select * from cat.deltadb1.itbl1.history").show()
//
//    spark.sql("select * from cat.deltadb1.itbl1").show()
//
//
//
//
//    replaceData1.write
//      .format("iceberg")
//      .saveAsTable("cat.deltadb1.itbl2")
//
//    spark.sql("select * from cat.deltadb1.itbl2.history").show()
//
//    spark.sql("select * from cat.deltadb1.itbl2").show()

//    replaceData1.write
//      .format("iceberg")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.itbl3")
//
//    replaceData.write
//      .format("iceberg")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.itbl3")


//    replaceData1.write
//      .format("iceberg")
//      .mode("error")
//      .saveAsTable("cat.deltadb1.itbl2")

//    spark.sql("select * from cat.deltadb1.itbl3.history").show()

  //  spark.sql("select * from cat.deltadb1.itbl3").show()
//    replaceData.write
//      .format("iceberg")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.itbl2")

   // spark.sql("select * from cat.deltadb1.itbl2.history").show()

//    replaceData1.write
//      .format("delta")
//      .mode("overwrite")
//      .saveAsTable("cat.deltadb1.dtbl2")
    // 3. Perform the selective overwrite
//    replaceData.write
//      .format("delta")
//      .mode("overwrite")
//      .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
//      .saveAsTable("cat.deltadb.tbl2")

//    replaceData.write
//      .format("delta")
//      .mode("overwrite")
//    //  .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
//      .saveAsTable("cat.deltadb1.dtbl2")

//    replaceData.write
//      .format("delta")
//      .mode("error")
//      //  .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
//      .saveAsTable("cat.deltadb1.dtbl2")

  //  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

//    replaceData.write
//      .format("delta")
//      .mode("overwrite")
//      .partitionBy("status")

      //  .option("replaceWhere", "start_date >= '2025-01-01' AND start_date <= '2025-01-31'")
   //   .saveAsTable("cat.deltadb1.dtbl3")


  //  val initialData = Seq(
//      (1, "Technology", 500, "2024-01-01"),
//      (2, "Furniture", 300, "2024-01-01"),
//      (3, "Office", 100, "2024-01-01")
//    ).toDF("id", "category", "amount", "date")
//
//    initialData.write.format("delta").mode("overwrite").saveAsTable("cat.deltadb1.dtbl4")

    // 3. Generate replaceData (The "New" Record for Technology)
    // Note: We are doubling the amount for ID 1
//    val replaceData2 = Seq(
//      (1, "Technology", 1000, "2024-01-01")
//    ).toDF("id", "category", "amount", "date")
//
//    // 4. Perform Selective Overwrite
//    replaceData2.write
//      .format("delta")
//      .mode("overwrite")
//      .option("replaceWhere", "category = 'Technology'")
//      .saveAsTable("cat.deltadb1.dtbl4")

//    val initialData = Seq(
//      (1, "Technology", 500, "2024-01-01"),
//      (2, "Furniture", 300, "2024-01-01"),
//      (3, "Office", 100, "2024-01-01")
//    ).toDF("id", "category", "amount", "date")

  //  initialData.write.format("delta").mode("append").saveAsTable("cat.deltadb1.dtbl4")



 //   spark.read.table("cat.deltadb1.dtbl4").show()





    /*metadata location management test*/
//    spark.sql("create database if not exists cat.mstbl")
//    spark.sql("create table cat.mstbl.itbl(id int, age int) using iceberg  PARTITIONED BY (age)")
//    spark.sql("create table cat.mstbl.itbl1(id int, age int) using iceberg  PARTITIONED BY (age) location '/tmp/ice1'")
//    spark.sql("insert into cat.mstbl.itbl values (1, 33), (2,34)")
//    spark.sql("insert into cat.mstbl.itbl1 values (1, 33), (2,34)")
//    val df = spark.sql("select * from cat.mstbl.itbl")
//    spark.sql("select * from cat.mstbl.itbl1").show()
//    df.write.format("iceberg").saveAsTable("cat.mstbl.itbl2")
//    spark.sql("select * from cat.mstbl.itbl2").show()
//    df.write.mode(SaveMode.Append).insertInto("cat.mstbl.itbl2")
//    spark.sql("select * from cat.mstbl.itbl2").show()


//    spark.sql("""create database if not exists cat.icebdb""")
//    spark.sql(
//      """CREATE TABLE cat.icebdb.icetbl (
//      id BIGINT,
//      data STRING,
//      state STRING,
//      ts TIMESTAMP
//    ) using iceberg partitioned by (state)""")
//
//    spark.sql("""alter table cat.icebdb.icetbl add partition field year(ts)""")
//
//    spark.sql(
//      """
//    INSERT INTO cat.icebdb.icetbl VALUES
//      (101, 'x', 'CA', timestamp'2025-01-03 00:00:00'),
//      (102, 'y', 'CA', timestamp'2025-01-03 01:00:00'),
//      (103, 'z', 'WA', timestamp'2025-01-04 00:00:00')
//    """)
//
//    spark.sql("""select * from cat.icebdb.icetbl""").show()
//    spark.sql("SELECT state, COUNT(*) c FROM cat.icebdb.icetbl GROUP BY state ORDER BY state").show()
//
//    spark.sql(
//      """CREATE TABLE cat.icebdb.icetbl1 (
//        |    uuid string NOT NULL,
//        |    level string NOT NULL,
//        |    age int,
//        |    range int)
//        |USING iceberg partitioned by (bucket(16,level))""".stripMargin)
//
//    spark.sql("insert into cat.icebdb.icetbl1 values('a', 'b',1,2), ('a1', 'b1',2,2)")
//    spark.sql("""select * from cat.icebdb.icetbl1""").show()
//





    //    spark.sql("create table cat.htbl.dtbl(id int, age int) using delta  PARTITIONED BY (age)")
//    spark.sql("insert into cat.htbl.dtbl values (1, 33), (2,34)")
//    spark.sql("select * from cat.htbl.dtbl").show()

    /* */

    /**current catalog test **/
//    spark.sql("use catalog cat")
//    spark.sql("create database cat.cudb")
//    spark.sql("create table cat.cudb.tb(id int, id1 int, name string) using parquet")
//    spark.sql("insert into cat.cudb.tb values (1,2, 'ss'), (2,3,'su')")
//    spark.sql("select name,id1 from cudb.tb").show()
    /**current catalog test **/


    /**Iceberg migration test starts**/

  //  spark.sql("create database if not exists cat.lidb2")


     /**UnPartitioned Manage table **/
//    spark.sql("create table cat.lidb.ptbl(id int) using parquet")
//    spark.sql("insert into cat.lidb.ptbl values (1), (2)")
//    spark.sql("select id from cat.lidb.ptbl").show()
//    spark.sql("CALL catalog_name.system.migrate('cat.lidb.ptbl')").show()
//
//    spark.sql("describe formatted cat.lidb.ptbl_BACKUP_").show()
//    spark.sql("select * from cat.lidb.ptbl_BACKUP_").show()
//
//    spark.sql("describe formatted cat.lidb.ptbl").show()
//    spark.sql("select * from cat.lidb.ptbl").show()
    /**UnPartitioned Manage table **/
//    spark.sql("create table cat.lidb.ptbl2(id int, age int) using iceberg  PARTITIONED BY (age) location '/tmp/ice'")
//    spark.sql("insert into cat.lidb.ptbl2 values (1, 33), (2,34)")
//    spark.read.table("cat.lidb.ptbl2").show()

    /**Partitioned table check **/
  //  spark.sql("create table cat.lidb.ptbl1(id int, age int) using iceberg PARTITIONED BY (age)")

//    spark.sql("create table cat.lidb2.ptbl2(id int, age int) using parquet PARTITIONED BY (age)")
//    spark.sql("insert into cat.lidb2.ptbl2 values (1, 33), (2,34)")
  //  val df = spark.read.table("cat.lidb1.ptbl2")
  //  df.write.format("iceberg").saveAsTable("cat.lidb1.itbl2")
  //  spark.read.table("cat.lidb1.itbl2").show()
  //  spark.sql("insert into cat.lidb1.itbl2 values(8,2)")
  //  spark.sql("ALTER TABLE cat.lidb1.itbl2 CREATE BRANCH `test-branch` RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS")
  //  spark.sql("create table cat.lidb2.itbl4(id int, age int) using iceberg PARTITIONED BY (age) location '/tmp/itbl3'")
//    spark.sql("insert into cat.lidb1.itbl4 values (1, 33), (2,34)")
//    spark.sql("describe formatted cat.lidb1.itbl4").show
//    spark.sql("select * from cat.lidb1.itbl4").show
//    spark.sql(
//      """
//        |call cat.system.add_files(
//        |table => 'cat.lidb2.itbl4',
//        |source_table => 'cat.lidb2.ptbl2'
//        |)
//        |""".stripMargin)
//
//    spark.sql("insert into cat.lidb2.itbl4 values (3,35)")
//    spark.read.table("cat.lidb2.itbl4").show()

//    spark.sql(
//      """
//        |CALL cat.system.snapshot(
//        |    source_table => 'lidb1.ptbl2',
//        |    table => 'cat.lidb1.itbl',
//        |    location => '/tmp/itbl',
//        |    properties => map('owner', 'data_team')
//        |)
//        |""".stripMargin)
////    spark.sql(
////            """
////              |CALL catalog_name.system.snapshot(
////              |    source_table => 'cat.lidb1.ptbl2',
////              |    table => 'cat.lidb1.itbl',
////              |    properties => map('owner', 'data_team')
////              |)
////              |""".stripMargin)
//
//    spark.sql("describe formatted cat.lidb1.itbl").show()
//    spark.read.table("cat.lidb1.itbl").show()
//    spark.sql("CALL cat.system.migrate('cat.lidb1.ptbl2')").show()
//    spark.read.table("cat.lidb1.ptbl2").show()

//    spark.sql("insert into cat.lidb.ptbl1 values (1, 33), (2,34)")
//    spark.sql("select * from cat.lidb.ptbl1").show()
//    spark.sql("select * from cat.lidb.ptbl1.history").show()
//    spark.sql("select * from cat.lidb.ptbl1.snapshots").show()
//    spark.sql("select * from cat.lidb.ptbl1.entries").show()
//    spark.sql("select * from cat.lidb.ptbl1.files").show()
//    spark.sql("select * from cat.lidb.ptbl1.position_deletes").show()
   // spark.sql("select * from cat.lidb.ptbl1.changes").show()
//    spark.sql("create table cat.lidb.ptbl1(id int, age int) using parquet PARTITIONED BY (age)")
//    spark.sql("insert into cat.lidb.ptbl1 values (1, 33), (2,34)")
//    spark.sql("select * from cat.lidb.ptbl1").show()
//    spark.sql("CALL cat.system.migrate('cat.lidb.ptbl1')").show()
//    spark.sql("describe formatted cat.lidb.ptbl1").show()
//    spark.sql("select * from cat.lidb.ptbl1").show()
//    spark.sql("insert into cat.lidb.ptbl1 values (3, 33), (4,34)")
    //Not working

   // spark.sql("use catalog cat")
   // spark.sql("use namespace lidb")
    //working procedure call
    //spark.sql("CALL cat.system.ancestors_of('lidb.ptbl1')").show()
    //spark.sql("CALL cat.system.create_changelog_view(table => 'lidb.ptbl1',options => map('start-snapshot-id','1','end-snapshot-id', '2'))")
//    spark.sql(
//      """
//        |CALL cat.system.register_table(
//        |  table => 'lidb.tbl',
//        |  metadata_file => '/tmp/ice/metadata/v2.metadata.json'
//        |)
//        |""".stripMargin)
//    spark.sql("select * from cat.lidb.tbl").show()
    /**Partitioned table check **/

    /**Iceberg migration test ends **/
    /**Listener test **/

//    spark.sql("create table cat.lidb.tbl(id int, name string) using iceberg")
//    spark.sql("select * from cat.lidb.tbl").show()
//
//    spark.sql("create table cat.lidb.tbl1 as select * from cat.lidb.tbl")
//    spark.sql("insert into cat.lidb.tbl values(1, 'sunny')")
//   // spark.sql("")
//    spark.sql("update  cat.lidb.tbl1 as t1 set name = 'Sunny Singh' where id = 1")



    /**Listener test end **/
//    var df = spark.read.option("inferSchema", "true").option("multiLine", "true").json("/Users/sharadsingh/Desktop/sample_json/")
//    df = df.selectExpr("Policy.`Party.Party` as party_details")
//    df = df.select(explode(col("party_details")).as("party_details_field"))
//    df = df.select($"party_details_field.*")
//    df.select(concat(col("PartyKey"), col("FullName")).as("new_col")).show(false)

    /*csv options*/
//    spark.sql("create database cat.csvdb2")
//    spark.sql("create table cat.csvdb2.ctbl(name string, id int,value int) using csv location '/tmp/csvt/' options('header' = 'true')")
//    val df = spark. read.table("cat.csvdb2.ctbl")
//    df.show()
//    println(df.count())
//    spark.sql("create database cat.pdb")
//    df5.writeTo("cat.pdb.tbl").using("delta").createOrReplace()
    //spark.sql("create table cat.padb.csvtbl1(id int, name string)  stored as parquet")
  //  spark.read.table("cat.pdb.csvtbl1").show()
//    spark.sql(
//      """
//        |CREATE EXTERNAL TABLE cat.csvdb.hive_format_table
//        |(no integer, name string)
//        |ROW FORMAT DELIMITED
//        |FIELDS TERMINATED BY ";"
//        |STORED AS TEXTFILE
//        |LOCATION '/tmp/csv1/'
//        |TBLPROPERTIES ("skip.header.line.count"="1");
//        |""".stripMargin)
//
//    spark.read.table("cat.csvdb.hive_format_table").show()
    /*csv options*/

    /*create iceberg table*/
//    spark.sql("create database cat.dbice")
//    spark.sql("create table cat.dbice.tbl(id int, name string) using iceberg")
//
//    spark.sql("describe formatted cat.dbice.tbl").show()
//    spark.sql("insert into cat.dbice.tbl values(1, 'sunny')")
//    df5.write.format("iceberg").mode("overwrite").saveAsTable("cat.dbice.tbl")
//    df5.write.format("iceberg").mode("append").saveAsTable("cat.dbice.tbl")
//    spark.sql("select * from cat.dbice.tbl").show()
//    spark.read.table("cat.dbice.tbl").show()
//    df4.write.format("iceberg").saveAsTable("cat.dbice.tbl1")
//    spark.read.table("cat.dbice.tbl1").show()
//    spark.sql("update cat.dbice.tbl as t1 set name = 'Sunny' where id = 7")
//    spark.sql("select * from cat.dbice.tbl").show()
//    spark.sql("delete from cat.dbice.tbl where where id = 12")
//    spark.sql("select * from cat.dbice.tbl").show()
//    println("History command coming!!")
//    spark.sql("select * from cat.dbice.tbl.history").show()
//    spark.sql("select * from cat.dbice.tbl.snapshots").show()
//    spark.sql("select * from cat.dbice.tbl.entries").show()
//    spark.sql("select * from cat.dbice.tbl.files").show()
//    spark.sql("select * from cat.dbice.tbl.position_deletes").show()
//
//
//    spark.sql("create database cat.dbice1")
//    spark.sql("create table cat.dbice1.tbl(id int, name string) using iceberg location '/tmp/tbl'")
//    spark.sql("describe formatted cat.dbice1.tbl").show()
//    spark.sql("insert into cat.dbice1.tbl values(1, 'sunny')")
//    df5.write.format("iceberg").mode("append").saveAsTable("cat.dbice1.tbl")
//    spark.sql("select * from cat.dbice1.tbl").show()
//    spark.sql("update cat.dbice1.tbl as t1 set name = 'Sunny' where id = 7")
//    spark.sql("select * from cat.dbice1.tbl").show()
//    spark.sql("delete from cat.dbice1.tbl where where id = 12")
//    spark.sql("select * from cat.dbice1.tbl").show()
//    println("History command coming!!")
//    spark.sql("select * from cat.dbice1.tbl.history").show()
//    spark.sql("select * from cat.dbice1.tbl.snapshots").show()
//    spark.sql("select * from cat.dbice1.tbl.files").show()
//    spark.sql("select * from cat.dbice1.tbl.position_deletes").show()


    /**tpcds query*/
//    spark.sql("create database cat.tpcds")
//    spark.sql(
//      """
//        |create table cat.tpcds.date_dim
//        |(
//        |d_date_sk                 int,
//        |d_date_id                 string,
//        |d_date                    date,
//        |d_month_seq               int,
//        |d_week_seq                int,
//        |d_quarter_seq             int,
//        |d_year                    int,
//        |d_dow                     int,
//        |d_moy                     int,
//        |d_dom                     int,
//        |d_qoy                     int,
//        |d_fy_year                 int,
//        |d_fy_quarter_seq          int,
//        |d_fy_week_seq             int,
//        |d_day_name                string,
//        |d_quarter_name            string,
//        |d_holiday                 string,
//        |d_weekend                 string,
//        |d_following_holiday       string,
//        |d_first_dom               int,
//        |d_last_dom                int,
//        |d_same_day_ly             int,
//        |d_same_day_lq             int,
//        |d_current_day             string,
//        |d_current_week            string,
//        |d_current_month           string,
//        |d_current_quarter         string,
//        |d_current_year            string
//        |) using parquet
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        create table cat.tpcds.web_sales
//        |(
//        |ws_sold_date_sk           int,
//        |ws_sold_time_sk           int,
//        |ws_ship_date_sk           int,
//        |ws_item_sk                int,
//        |ws_bill_customer_sk       int,
//        |ws_bill_cdemo_sk          int,
//        |ws_bill_hdemo_sk          int,
//        |ws_bill_addr_sk           int,
//        |ws_ship_customer_sk       int,
//        |ws_ship_cdemo_sk          int,
//        |ws_ship_hdemo_sk          int,
//        |ws_ship_addr_sk           int,
//        |ws_web_page_sk            int,
//        |ws_web_site_sk            int,
//        |ws_ship_mode_sk           int,
//        |ws_warehouse_sk           int,
//        |ws_promo_sk               int,
//        |ws_order_number           long,
//        |ws_quantity               int,
//        |ws_wholesale_cost         decimal(7,2),
//        |ws_list_price             decimal(7,2),
//        |ws_sales_price            decimal(7,2),
//        |ws_ext_discount_amt       decimal(7,2),
//        |ws_ext_sales_price        decimal(7,2),
//        |ws_ext_wholesale_cost     decimal(7,2),
//        |ws_ext_list_price         decimal(7,2),
//        |ws_ext_tax                decimal(7,2),
//        |ws_coupon_amt             decimal(7,2),
//        |ws_ext_ship_cost          decimal(7,2),
//        |ws_net_paid               decimal(7,2),
//        |ws_net_paid_inc_tax       decimal(7,2),
//        |ws_net_paid_inc_ship      decimal(7,2),
//        |ws_net_paid_inc_ship_tax  decimal(7,2),
//        |ws_net_profit             decimal(7,2)
//        |)
//        |USING parquet
//        |partitioned by (ws_sold_date_sk)
//        |
//        |""".stripMargin)
//
//
//    spark.sql(
//      """
//        |
//        |create table cat.tpcds.catalog_sales
//        |(
//        |cs_sold_date_sk           int,
//        |cs_sold_time_sk           int,
//        |cs_ship_date_sk           int,
//        |cs_bill_customer_sk       int,
//        |cs_bill_cdemo_sk          int,
//        |cs_bill_hdemo_sk          int,
//        |cs_bill_addr_sk           int,
//        |cs_ship_customer_sk       int,
//        |cs_ship_cdemo_sk          int,
//        |cs_ship_hdemo_sk          int,
//        |cs_ship_addr_sk           int,
//        |cs_call_center_sk         int,
//        |cs_catalog_page_sk        int,
//        |cs_ship_mode_sk           int,
//        |cs_warehouse_sk           int,
//        |cs_item_sk                int,
//        |cs_promo_sk               int,
//        |cs_order_number           long,
//        |cs_quantity               int,
//        |cs_wholesale_cost         decimal(7,2),
//        |cs_list_price             decimal(7,2),
//        |cs_sales_price            decimal(7,2),
//        |cs_ext_discount_amt       decimal(7,2),
//        |cs_ext_sales_price        decimal(7,2),
//        |cs_ext_wholesale_cost     decimal(7,2),
//        |cs_ext_list_price         decimal(7,2),
//        |cs_ext_tax                decimal(7,2),
//        |cs_coupon_amt             decimal(7,2),
//        |cs_ext_ship_cost          decimal(7,2),
//        |cs_net_paid               decimal(7,2),
//        |cs_net_paid_inc_tax       decimal(7,2),
//        |cs_net_paid_inc_ship      decimal(7,2),
//        |cs_net_paid_inc_ship_tax  decimal(7,2),
//        |cs_net_profit             decimal(7,2)
//        |)
//        |USING parquet
//        |partitioned by (cs_sold_date_sk)
//        |
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |with wscs as
//        | (select sold_date_sk
//        |        ,sales_price
//        |  from  (select ws_sold_date_sk sold_date_sk
//        |              ,ws_ext_sales_price sales_price
//        |        from cat.tpcds.web_sales
//        |        union all
//        |        select cs_sold_date_sk sold_date_sk
//        |              ,cs_ext_sales_price sales_price
//        |        from cat.tpcds.catalog_sales) x ),
//        | wswscs as
//        | (select d_week_seq,
//        |        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
//        |        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
//        |        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
//        |        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
//        |        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
//        |        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
//        |        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
//        | from wscs
//        |     ,cat.tpcds.date_dim
//        | where d_date_sk = sold_date_sk
//        | group by d_week_seq)
//        | select d_week_seq1
//        |       ,round(sun_sales1/sun_sales2,2)
//        |       ,round(mon_sales1/mon_sales2,2)
//        |       ,round(tue_sales1/tue_sales2,2)
//        |       ,round(wed_sales1/wed_sales2,2)
//        |       ,round(thu_sales1/thu_sales2,2)
//        |       ,round(fri_sales1/fri_sales2,2)
//        |       ,round(sat_sales1/sat_sales2,2)
//        | from
//        | (select wswscs.d_week_seq d_week_seq1
//        |        ,sun_sales sun_sales1
//        |        ,mon_sales mon_sales1
//        |        ,tue_sales tue_sales1
//        |        ,wed_sales wed_sales1
//        |        ,thu_sales thu_sales1
//        |        ,fri_sales fri_sales1
//        |        ,sat_sales sat_sales1
//        |  from wswscs,cat.tpcds.date_dim
//        |  where cat.tpcds.date_dim.d_week_seq = wswscs.d_week_seq and
//        |        d_year = 2001) y,
//        | (select wswscs.d_week_seq d_week_seq2
//        |        ,sun_sales sun_sales2
//        |        ,mon_sales mon_sales2
//        |        ,tue_sales tue_sales2
//        |        ,wed_sales wed_sales2
//        |        ,thu_sales thu_sales2
//        |        ,fri_sales fri_sales2
//        |        ,sat_sales sat_sales2
//        |  from wswscs
//        |      ,cat.tpcds.date_dim
//        |  where cat.tpcds.date_dim.d_week_seq = wswscs.d_week_seq and
//        |        d_year = 2001+1) z
//        | where d_week_seq1=d_week_seq2-53
//        | order by d_week_seq1
//        |""".stripMargin).show()


    /**tpcds query*/
    /*** perf opt***/

//    spark.sql("create database cat.dbopt")
//    spark.sql("create table cat.dbopt.tbl(id int, name string, city string) using parquet partitioned by(city) ")
//    spark.sql("insert into cat.dbopt.tbl values(1, 'sharad', 'bng'), (2, 'xiaoyu', 'sfo'), (3, 'shashi', 'sfo'), (4, 'ram', 'bng')")
//    spark.sql("select * from (select sum(id) as sid, city from cat.dbopt.tbl group by city)").show()
//    spark.sql("select * from cat.dbopt.tbl").show()
//    spark.sql("select * from cat.dbopt.tbl values where city = 'bng' ").show()
//    spark.sql("select min(id) as sid, name from cat.dbopt.tbl group by name").show()


//    spark.sql("analyze table cat.dbopt.tbl COMPUTE STATISTICS for all columns")
 //   val plugin = spark.sessionState.catalogManager.catalog("cat")
 //   val tid = TableIdentifier(table = "tbl", database = Some("dbopt"), catalog = Some("cat"))
   // AnalyzeCommandUtil.analyzeTable(sparkSession = spark, tableIdent = tid, plugin = plugin)
  //  AnalyzeCommandUtil.analyzeColumnInCatalog(spark, "cat", "dbopt","tbl", None, true)

   // spark.sql("select sum(id) as sid, city from cat.dbopt.tbl group by city").show()
  //  spark.sql("select sum(id) as sid, name from cat.dbopt.tbl group by name").show()

    /*** perf opt***/
  //  position_deletes
    //    spark.sql("describe formatted cat.dbice.tbl").show()
    //    spark.sql("create table cat.dbice.tbl(id i
   // spark.sql("select * from cat.dbice.tbl.snapshots").show()


    /*create iceberg table ends*/

    /*Delta overwrite table*/
    //    spark.sql("create database cat.db6")
    //    spark.sql("create table cat.db6.t(id int, name string)")
    //    spark.sql("insert into cat.db6.t values (1, 'ss'), (2, 'xy')")
    //    spark.sql("create table cat.db6.ctas as select * from cat.db6.t")
    //    spark.sql("create table cat.db6.pt using parquet as select * from cat.db6.t")

    //   spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    //   spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")
    //   // spark.sql.exchange.reuse
    //    spark.conf.set("spark.sql.exchange.reuse", "false")
//        spark.sql("create database cat.db7")
//        spark.sql("create table cat.db7.t1(id int, name string, city string) using parquet partitioned by(city)")
//        spark.sql("create table cat.db7.t2(id int, name string, city string) partitioned by(city)")
//        spark.sql("insert into cat.db7.t1 values (1, 'ss', 'vns'), (2, 'ash', 'vns'), (1, 'xy', 'sea'), (2, 'jhn', 'sea')")
//        spark.sql("insert into cat.db7.t2 values (1, 'ss', 'vns'), (2, 'ash', 'vns'), (1, 'xy', 'sea'), (2, 'jhn', 'sea')")
//        val df = spark.sql("select  distinct cat.db7.t1.id, cat.db7.t1.name from cat.db7.t1 LEFT SEMI JOIN cat.db7.t2 on cat.db7.t1.city = cat.db7.t2.city and cat.db7.t1.id<2 and cat.db7.t1.name = 'ss'")
//        df.explain(true)
//        println("-----Plan serialize------")
//        df.show()
//        df.write.saveAsTable("cat.db7.t4")
//        spark.sql("create table cat.db7.t3 as select distinct cat.db7.t1.id, cat.db7.t1.name from cat.db7.t1 LEFT SEMI JOIN cat.db7.t2 on cat.db7.t1.city = cat.db7.t2.city and cat.db7.t1.id<2 and cat.db7.t1.name = 'ss'")
    //
    ////
    ////    /** distinct operation* */
    //    spark.sql("create database cat.dml")
    //    spark.sql("create table cat.dml.t2(id string, new_val int) using parquet")
    //    spark.sql("insert into cat.dml.t2 values('hello', 1), ('hi', 2)")
    //    val df9 = spark.sql("select distinct id, new_val from cat.dml.t2")
    //    spark.sql("create table cat.dml.td as select distinct id, new_val from cat.dml.t2")
    //    //df9.show()
    //    df9.write.format("delta").saveAsTable("cat.dml.t3")
    ////
    ////
    //    val df6 = spark.sql("SELECT DISTINCT c.tenant, c.personid FROM (SELECT tenant, personid FROM values (1, 1), (2, 2), (0, 1) as tab(tenant, personid)) c, (SELECT personid FROM values (1) as tab(personid)) b WHERE b.personid = c.personid group by c.tenant, c.personid")
    //    //val logical = df.queryExecution.optimizedPlan
    //    //print(s"###plan is ${logical.isInstanceOf[Aggregate]}")
    //    val tableName = "cat.dml.test_table_distinct1"
    //    df6.write.format("delta").mode("append").saveAsTable(tableName)
    //
    //    val df10 = spark.sql("SELECT DISTINCT col1 FROM values (1), (2), (1) as tab(col1)")
    //   // val logical = df.queryExecution.optimizedPlan
    //   // assert(logical.isInstanceOf[Aggregate])
    //    val tableName1 = "cat.dml.test_table_distinct2"
    //    df10.write.format("parquet").mode("overwrite").saveAsTable(tableName1)

    /* catalog rename*/
//    spark.sql("create database hive.db19")
//    df3.write.mode("overwrite").saveAsTable("hive.db19.tbl")
//    val df_1 = spark.read.table("hive.db19.tbl")
//    df_1.show()
//    spark.sql("describe formatted cat.db19.tbl").show()
//    spark.sql("describe formatted hive.db19.tbl").show()
    /*catalog rename ends*/
    /** disctinct operation ends* */
    //    spark.sql("create database cat.db9")
    //
    //    df3.write.mode("overwrite").saveAsTable("cat.db9.tbl")
    //    val df_1 = spark.read.table("cat.db9.tbl")
    //    df_1.show()
    //    df_1.write.mode("overwrite").format("delta").saveAsTable("cat.db9.tbl1")
    //    df_1.write.mode("overwrite").format("delta").saveAsTable("cat.db9.tbl1")
    //    df_1.write.insertInto("cat.db9.tbl1")
    //    val df_6 = spark.read.table("cat.db9.tbl1")
    //    df_6.show()
    //    spark.sql("create table cat.db9.tble(col1 int, col2 string, col3 double) using delta location '/tmp/dx'")
    //    df3.write.insertInto("cat.db9.tble")
    //    val df_7 = spark.read.table("cat.db9.tble")
    //    df_7.show()
    //    df3.write.mode("overwrite").format("delta").saveAsTable("cat.db5.tbl")
    //    val df6 = spark.read.table("cat.db5.tbl")
    //    df6.show()

    //spark.sql
    /*Delta overwrite table*/
    //
    //  //  df3.write.options(Map("k3"->"v3")).mode(SaveMode.Append).insertInto("cat.customdb.tbl")
    //  //  spark.read.options(Map("k4"->"v4")).table("cat.customdb.tbl").show()
   // spark.sql("create database ecat.customdb1")
   // spark.sql("create table ecat.customdb1.nt(col1 int, col2 string,col3 int) using custom  options('table' = 'NT', 'schema' = 'CUSTOMDB')")
//    val providedSchema = df5.schema
    //    df3.write.options(Map("k5"->"v5")).mode(SaveMode.Overwrite).saveAsTable("ecat.customdb.tbl")
   // spark.read.option("source.pushdown.enabled","true").options(Map("k6"->"v6", "override" -> "true")).table("ecat.customdb1.nt").show()
//    //    df3.write.mode(SaveMode.Overwrite).saveAsTable("ecat.customdb.tbl1")
//    //    df3.write.mode(SaveMode.Overwrite).saveAsTable("ecat.customdb.nt")
//    df3.write.mode("overwrite").saveAsTable("ecat.customdb1.nt")
//
//    //    df3.write.format("custom").mode(SaveMode.Overwrite).saveAsTable("ecat.customdb.tbl1")
//    spark.read.options(Map("k6"->"v6")).table("ecat.customdb1.nt").show()
    //    df3.write.mode(SaveMode.Overwrite).saveAsTable("ecat.customdb.tbl2")
    //    df3.write.saveAsTable("ecat.customdb.tbl2")
    //    spark.sql("describe formatted ecat.customdb.tbl2").show()


    /** Custom data format  write options ends* */


    /** View DDL Compiler starts* */
//        spark.sql("create database cat.viewdb1")
//        spark.sql("create table cat.viewdb1.t2(attributes string) using avro")
//        spark.sql("create table cat.viewdb1.t3(id int, name string) using delta")
//        spark.sql("insert into cat.viewdb1.t2 values('hello')")
//        spark.sql("insert into cat.viewdb1.t3 values(1,'hello')")
//        spark.sql("create view cat.viewdb1.v1(cl1) as select attributes from cat.viewdb1.t2")
//        spark.sql("show views in cat.viewdb1").show()
//       // spark.sql("drop view cat.viewdb1.v1")
//        spark.sql("alter view cat.viewdb1.v1 set TBLPROPERTIES ('k' = 'v')")
//        spark.sql("alter view cat.viewdb1.v1 as select * from cat.viewdb1.t3")
//     spark.sql("describe formatted cat.viewdb1.v1").show(40, false)
//      spark.sql("select * from cat.viewdb1.v1").show()
//        spark.sql("alter view cat.viewdb1.v1 UNSET TBLPROPERTIES if exists ('k')")
//        spark.sql("alter view cat.viewdb1.v1 rename to cat.viewdb1.v2")
//        spark.sql("describe formatted cat.viewdb1.v1").show(40, false)
//        spark.sql("select * from cat.viewdb1.v2")


    /** View DDL Compile ends r* */


    /** View fix start* */
    //    spark.sql("create database cat.viewdb")
    //    spark.sql("create table cat.viewdb.t2(attributes string) using avro")
    //    spark.sql("insert into cat.viewdb.t2 values('hello')")
    //    spark.sql("create view cat.viewdb.v1(cl1) as select * from cat.viewdb.t2")
    //    spark.sql("select  concat(cl1, ' hi') as c6, cl1, 1 as cl1 from cat.viewdb.v1").show()
    //    spark.sql("describe formatted cat.viewdb.v1").show()
    //    spark.sql("describe formatted cat.viewdb.t2").show()
    //    spark.sql("describe table cat.viewdb.v1").show()
    //    spark.sql("ALTER TABLE  cat.viewdb.v1 SET TBLPROPERTIES('k' = 'v')")
    //  spark.sql("ALTER VIEW  cat.viewdb.v1 SET TBLPROPERTIES('k' = 'v')")
    //    spark.sql("CREATE TABLE cat.viewdb.dealer (id INT, city STRING, car_model STRING, quantity INT)")
    //    spark.sql("""INSERT INTO cat.viewdb.dealer VALUES
    //                |    (100, 'Fremont', 'Honda Civic', 10),
    //                |    (100, 'Fremont', 'Honda Accord', 15),
    //                |    (100, 'Fremont', 'Honda CRV', 7),
    //                |    (200, 'Dublin', 'Honda Civic', 20),
    //                |    (200, 'Dublin', 'Honda Accord', 10),
    //                |    (200, 'Dublin', 'Honda CRV', 3),
    //                |    (300, 'San Jose', 'Honda Civic', 5),
    //                |    (300, 'San Jose', 'Honda Accord', 8)""".stripMargin)
    //    spark.sql("SELECT city, sum(quantity) AS sum FROM cat.viewdb.dealer GROUP BY city HAVING city = 'Fremont'").show()
    //    spark.sql("SELECT case when  sum(quantity)>10 then 'bigger' else 'small' end as status, city, sum(quantity) AS sum FROM cat.viewdb.dealer GROUP BY city HAVING sum(quantity)>5").show()
    //    spark.sql("drop table cat.viewdb.v1").show()

    /** View fix end* */

    /** *query model with structs starts* */

    // spark.sql("create database cat.ai")
    //  spark.sql("create table cat.ai.t2(attributes string)")
    //  spark.sql("insert into cat.ai.t2 values('hello')")
    //   spark.sql("create table cat.ai.t1(attributes STRUCT<height: DOUBLE, weight: FLOAT, eye_color: STRING>)")
    //  spark.sql("insert into cat.ai.t1 values(named_struct('height',5.9, 'weight', 180.5, 'eye_color', 'brown'))")
    // spark.sql("select * from cat.ai.t1").show()
    //  spark.sql("select query_model('meta', attributes) from cat.ai.t2").show()
    //   spark.sql("select query_model('meta', attributes) from cat.ai.t1").show

    /** *query model with structs ends * */

    /** spark csv table properties starts* */
    //    spark.sql("create database if not exists cat.csvdb")
    //    spark.sql("create table cat.csvdb.csvtbl(id int, name string)  using csv  location '/tmp/csv/' TBLPROPERTIES('hasheaders' = 'true')")
    //   // spark.read.table("cat.csvdb.csvtbl").show
    //
    //    spark.sql("insert into cat.csvdb.csvtbl values(9,'str'), (10, 'st2')")
    //    spark.sql("select * from cat.csvdb.csvtbl").show()
    //
    //    spark.sql("create table cat.csvdb.csvtbl1(id int, name string)  using csv  location '/tmp/csv1/' TBLPROPERTIES('field.delim' = ';','hasheaders' = 'true' )")
    //    spark.sql("insert into cat.csvdb.csvtbl1 values(9,'str'), (10, 'st2')")
    //    spark.sql("select * from cat.csvdb.csvtbl1").show()


    /** spark csv table properties ends * */


    // import spark.sqlContext.implicits._


    // val hpl = CustomSparkSQLParser.parsePlan("describe history cat.db.tb")


    //  val cbpl = CustomSparkSQLParser.parsePlan("create table cat.db.tb(id int, name string) using delta cluster by(id)")

    //  cbpl


//     spark.sql("create database lsdb2")
//    spark.sql("set spark.sql.parquet.compression.codec=lz4raw")
//    val df2 = Seq(
//      "John",
//      "Sunny",
//      "Xiaoyu",
//      "Shashi",
//      "Bharath",
//      "Vivek"
//    ).toDF("col1")


//    val df1 = Seq(
//      (1, 2),
//      (2, 3),
//      (3, 4)
//    ).toDF("id", "id1")

    //    spark.sql("create database cat.tdb3")
    //    df1.write.saveAsTable("cat.tdb3.tbl")
    //    df1.write.saveAsTable("cat.tdb3.tbl1")
    //    df1.write.format("delta").saveAsTable("cat.tdb3.tbl2")


    //    spark.sql("refresh schema in external catalog cat.ab").show()
    //    spark.sql("refresh table in external catalog cat.ab.cd").show()

    //    spark.sql("create database cat.tdp4")
    //    spark.sql("create table cat.tdp4.t1(id int, country string, city string) location '/tmp/tp'")
    //    spark.sql("insert into cat.tdp4.t1 values (1, 'India','Pune'), (2, 'India','Bangalore'), (3, 'India','Mumbai'), (4, 'India','Delhi')")
    //    spark.sql("select * from cat.tdp4.t1").show()
    // spark.sql("select * from cat.tdb3.tbl").show()
    //   spark.read.table("cat.tdb3.tbl").show()

    //    spark.read.table("cat.tdb3.tbl2").show()
    //
    //    spark.sql("create view cat.tdb3.v1(id, id1) as select * from cat.tdb3.tbl")
    //    spark.sql("select * from cat.tdb3.v1").show()
    //

    //   spark.sql("""create function row_func for table cat.tdb3.tbl where 'id>2' """)
    //   spark.sql("grant row_level row_func for user userX")
    //
    //    println("tbl Output after sec")
    //   spark.sql("select * from cat.tdb3.tbl").show()
    //
    //   spark.sql("create view cat.tdb3.v1(id, id1) as select * from cat.tdb3.tbl1")
    //
    //    spark.sql("""create function row_func1 for table cat.tdb3.v1 where 'id>2' """)
    //    spark.sql("grant row_level row_func1 for user userX")
    //
    //    spark.sql("""create function row_func2 for table cat.tdb3.tbl2 where 'id>2' """)
    //    spark.sql("grant row_level row_func2 for user userX")
    //
    //    println("delta table output after sec")
    //    spark.read.table("cat.tdb3.tbl2").show()
    //    spark.sql("select * from cat.tdb3.tbl2").show()
    //
    //
    //    println("View Output after sec")
    //    spark.sql("select * from cat.tdb3.v1").show()

    // spark.sql("select * from cat.tdb3.tbl1").show()
    //
    //    spark.sql("create materialized view cat.tdb3.mv tblproperties('schedular' = '1 day') as select * from cat.tdb3.tbl1")
    //    spark.sql("select * from cat.tdb3.mv").show()
    //    spark.sql("describe formatted cat.tdb3.mv").show(40, false)
    //    spark.sql("insert into cat.tdb3.tbl1 values (9,6)")
    //    spark.sql("refresh materialized view cat.tdb3.mv")
    //    spark.sql("select * from cat.tdb3.mv").show()

    //    spark.sql("create database cat.arrowdb")
    //    spark.sql("create table cat.arrowdb.tp(id int, name string) using parquet location '/tmp/parquet'")
    //  //  spark.sql("insert into cat.arrowdb.tp values(1,'sharad'), (2,'xiaoyu'), (3, 'bharat'), (4, 'shashi'), (5, 'vivek')")
    //    spark.sql("create table cat.arrowdb.ta(id int, name string) using arrow location '/tmp/parquet'")
    //    spark.sql("select * from cat.arrowdb.ta").show()


    //    spark.sql("create database cat.tdb2")
    //    spark.sql("create table cat.tdb2.etbl(id int, l2 string, l3 string)  using delta location '/tmp/tbl'")
    //    spark.read.table("cat.tdb2.etbl").show()
    //    spark.sql("select * from cat.tdb2.etbl").show()

    //    df1.write.mode("Overwrite").format("delta").saveAsTable("cat.tdb1.tbl")
    //    spark.read.table("cat.tdb1.tbl").show()
    //    df2.write.mode("Overwrite").format("delta").saveAsTable("cat.tdb1.tbl")
    //    spark.read.table("cat.tdb1.tbl").show()
    //    spark.sql("select * from cat.tdb1.tbl version as of 1").show()
    //    val dfx = spark.read.format("delta").option("versionAsOf",0).table("cat.tdb1.tbl")
    //    dfx.explain(true)
    //    dfx.show()


    //    df1.write.mode("Overwrite").format("parquet").saveAsTable("cat.tdb.ptbl")
    //    spark.read.table("cat.tdb.ptbl").show()
    //    df2.write.mode("Overwrite").format("parquet").saveAsTable("cat.tdb.ptbl")
    //    spark.read.table("cat.tdb.ptbl").show()
    //
    //
    //    df1.write.mode("Overwrite").format("avro").saveAsTable("cat.tdb.atbl")
    //    spark.read.table("cat.tdb.atbl").show()
    //    df2.write.mode("Overwrite").format("avro").saveAsTable("cat.tdb.atbl")
    //    spark.read.table("cat.tdb.atbl").show()
    //
    //
    //    df1.write.mode("Overwrite").format("orc").saveAsTable("cat.tdb.otbl")
    //    spark.read.table("cat.tdb.otbl").show()
    //    df2.write.mode("Overwrite").format("orc").saveAsTable("cat.tdb.otbl")
    //    spark.read.table("cat.tdb.otbl").show()
    //   // spark.sql("create table cat.tdb.etbl(id int, name string) using delta location '/tmp/dt'")
    //  //  spark.sql("create table cat.tdb.etbl1 using delta location '/tmp/dt'")

    /*nested aggregate function */
    //    spark.sql("create database cat.dbx122")
    //    spark.sql("create table cat.dbx122.tbl(pc int, fare double, distance long) using csv")
    //    spark.sql("insert into cat.dbx122.tbl values(1,12.5,200), (2,26.0,201), (3,29.8,300)")
    //    spark.sql("select * from cat.dbx122.tbl").show()
    //    spark.sql("select pc, round(sum(fare),0) as tf from cat.dbx122.tbl group by pc").show()


    /*nested aggregated function ends*/

    /** parquet double and float data types reader * */


    //    spark.sql("create database if not exists cat.dbx123")
    //    spark.sql("create table cat.dbx123.tbl(pc int, fare float, distance long) using csv")
    //    spark.sql("insert into cat.dbx123.tbl values(1,12.4,200), (2,26.5,201), (3,29.8,300)")
    //
    ////    spark.sql("select pc, fare, distance from cat.dbx123.tbl").show()
    //
    //    spark.sql("create table cat.dbx123.tbl1(pc int, fare float, distance long) using parquet")
    //    spark.sql("insert into cat.dbx123.tbl1 values(1,12.4,200), (2,26.5,201), (3,29.8,300)")
    //  //  val df = spark.read.format("parquet").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/dbx123.db/tbl1")
    // //   df.show()
    //   val df = spark.sql("select * from cat.dbx123.tbl")

    //  println("Pre overwrite of line 113")
    //    df.show()
    //    spark.sql(
    //      """
    //        |with t as (
    //        |select pc, fare from cat.dbx123.tbl1
    //        |)
    //        |select count(min(pc)) as cnt from t
    //        |""".stripMargin).show()
    //    df.write.mode(SaveMode.Overwrite).saveAsTable("cat.dbx123.tbl")
    //   println("post overwrite of line 113")
    //   spark.sql("select * from cat.dbx123.tbl9").show()
    //   spark.read.table("cat.dbx123.tbl1").show()
    //
    //
    //    spark.sql("create database if not exists cat.dbx124")
    //    spark.sql("create table cat.dbx124.tbl1(pc int, fare float, distance long) using delta")
    //    spark.sql("insert into cat.dbx124.tbl1 values(1,12.4,200), (2,26.5,201), (3,29.8,300)")
    //    println("Pre overwrite of line 122")
    //    spark.sql("select * from cat.dbx124.tbl1").show()
    //    df.write.mode(SaveMode.Overwrite).format("delta").saveAsTable("cat.dbx124.tbl1")
    //    df.write.mode(SaveMode.Append).format("delta").saveAsTable("cat.dbx124.tbl1")
    //    println("post overwrite of line 122")
    //    spark.sql("select * from cat.dbx124.tbl1").show()
    //    //    df.withColumn("new_fare", df.col("fare").cast(DecimalType(3, 1))).show()
    //    /**parquet double and float data types reader ends**/
    //
    //    /***date and timestamps insertion starts***/
    //
    //    spark.sql("create database if not exists cat.dbx125")
    //    spark.sql("create table cat.dbx125.tbl1(pc date, pc1 timestamp) using delta")
    //    spark.sql("create table cat.dbx125.tbl2(pc date, pc1 timestamp) using parquet")
    // spark.sql("insert into cat.dbx125.tbl1 values(DATE'2022-10-01', TIMESTAMP'2022-10-01 10:15:30'), (DATE'2022-10-02', TIMESTAMP'2022-10-02 10:15:30), (DATE'2022-10-03', TIMESTAMP'2022-10-03 10:15:30)")
    //    spark.sql("insert into cat.dbx125.tbl1 values(DATE'2022-10-01',cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp)), (DATE'2022-10-02',cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp)), (DATE'2022-10-03', cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp))")
    //    spark.sql("insert into cat.dbx125.tbl2 values(DATE'2022-10-01',cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp)), (DATE'2022-10-02',cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp)), (DATE'2022-10-03', cast(date_format('2019-06-13 13:22:30.521000000', 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp))")
    //    spark.sql("select * from cat.dbx125.tbl1").show()
    //    spark.sql("select * from cat.dbx125.tbl2").show()
    //    spark.sql("insert into cat.dbx125.tbl2 from cat.dbx125.tbl1 select pc, pc1")
    //    spark.sql("select * from cat.dbx125.tbl2").show()
    //    spark.sql("create database if not exists cat.dbx126")
    //    spark.sql("CREATE TABLE cat.dbx126.delta_test_table ( id INT, name STRING, birth_date DATE, created_at TIMESTAMP, is_active BOOLEAN, salary DECIMAL(10,2), profile BINARY, preferences ARRAY<STRING>, metadata MAP<STRING, STRING>, attributes STRUCT<height: DOUBLE, weight: FLOAT, eye_color: STRING>, big_number BIGINT, small_number SMALLINT, tiny_number TINYINT ) USING DELTA")
    //    spark.sql("""INSERT INTO cat.dbx126.delta_test_table VALUES ( 1, 'John Doe', '1990-05-15', current_timestamp(), true, 75000.50, X'68656C6C6F',  ARRAY('red', 'blue', 'green'), MAP('key1', 'value1', 'key2', 'value2'), STRUCT(5.9, 180.5, 'brown'), 23,3,1 )""")
    //    spark.read.table("cat.dbx126.delta_test_table").show()


    //    spark.sql("create database if not exists cat.dbx127")
    //    spark.sql("""create table cat.dbx127.catalog_returns_external
    //                |(
    //                |
    //                |cr_returned_time_sk int,
    //                |cr_net_loss decimal(7,2),
    //                |cr_returned_date_sk int
    //                |)
    //                |USING parquet
    //                |partitioned by (cr_returned_date_sk)""".stripMargin)
    //    spark.sql("INSERT INTO cat.dbx127.catalog_returns_external VALUES (37228, 990.23,2334254),(71132, 81.88,23424),(64324, 680.24,335325)")
    //    spark.sql("select * from cat.dbx127.catalog_returns_external")

    /** *date and timestamps insertion ends** */

    /** *savAsTable for external datasource starts** */


    /** *savAsTable for external datasource ends** */

    /** *Proxy Catalog start** */
    //    spark.sql("SHOW SCHEMAS IN cat").show(200, false)
    //    spark.sql("describe database cat.reservedb").show
    //    spark.sql("show tables in cat.reservedb").show
    //    spark.sql("describe extended cat.reservedb.resevetbl").show()
    /** **Proxy catalog end** */
    //  df1.withColumn("col3", expr("fadd('',col1, col2)")).show()
    //  df1.selectExpr("fadd('',col1, col2)").show

    // spark.sql("select fadd('a',  named_struct('a', 1, 'b', 2, 'c', 3),  named_struct('a', 2, 'b', 6, 'c', 3))").show(40,false)

    //    val codegenContext = new CodegenContext
    //    val exprCode = expr("fadd('',1, 2)").expr.genCode(codegenContext)
    //    println(exprCode.code.toString())
    //  spark.sql("select fadd('', 1, 2) from values (1, 2, 3)").show()

    //        spark.sql("create database cat.dbx118");
    //        df1.write.format("parquet").mode("overwrite").saveAsTable("cat.dbx118.json_tbl")
    //        df1.write.format("parquet").mode(SaveMode.Append).insertInto("cat.test_sudeep.jsonn_tbl")
    ////        spark.read.table("cat.test_sudeep.jsonn_tbl").show()
    //        spark.sql("select * from cat.test_sudeep.jsonn_tbl").show()

    //        val df2 = Seq(
    //          6,
    //          7
    //        ).toDF("col1")

    /** gen_ai */

    //  spark.sql("select query_model('meta', concat('his', 'tory' ))").show()
    // spark.sql("create database cat.gen_ai")
    // spark.sql("create table cat.gen_ai.tbl(c1 string) using delta")
    //    spark.sql("insert into cat.gen_ai.tbl values ('sharad'), ('sunny')")
    //  spark.sql("select query_model('a1', 'values') as c2").show
    //    spark.sql("select query_model('meta',c1) as cai from cat.gen_ai.tbl").show()
    //  spark.sql("""select query_model('meta',concat(c1, "hello")) as c11 from cat.gen_ai.tbl""").show()


    /** gen_ai end */

    /** custom datasource write and read patterns patterns starts* */

    //  spark.sql("create database cat.customdb")
    // spark.sql("create table cat.customdb.tbl(c1 string) using custom options('k'='v', 'k1' = 'v1')")
    // spark.sql("insert into cat.customdb.tbl values('hello'), ('hi') ")
    //val df = spark.read.table("cat.customdb.tbl")
    // df.show()
    // df.write.insertInto("cat.customdb.tbl")
    //  df.write.mode(SaveMode.Append).saveAsTable("cat.customdb.tbl")
    //  df2.write.mode(SaveMode.Overwrite).format("custom").saveAsTable("cat.customdb.tbl")
    // spark.sql("select * from cat.customdb.tbl").show()

    /** custom datasource write and read patterns patterns ends* */
    // spark.sql("create database cat.test_sudeep");

    //    df1.write.format("delta").saveAsTable("cat.dbx122.delta_tbl")
    //    spark.sql("describe formatted cat.dbx122.delta_tbl").show()
    //    spark.sql("describe formatted cat.dbx122.json_tbl").show()
    //    df1.write.format("parquet").mode("overwrite").saveAsTable("cat.test_sudeep.json_tbl")
    //    df1.write.format("parquet").mode("append").insertInto("cat.test_sudeep.json_tbl")
    //    spark.read.table("cat.test_sudeep.json_tbl").show()
    //    spark.sql("select * from cat.test_sudeep.json_tbl").show()
    //    spark.sql("insert into cat.test_sudeep.json_tbl values (5,8), (4,7), (6,9)")
    //    spark.sql("select * from cat.test_sudeep.json_tbl where col1>4").show()
    //    spark.sql("create view cat.test_sudeep.v1(cl1, cl2) as select * from cat.test_sudeep.json_tbl where col1>4")
    //
    //   // spark.sql("select * from cat.test_sudeep.json_tbl limit 2").show()
    //    spark.sql("select cl1 from cat.test_sudeep.v1 limit 2").show()
    //        spark.sql("drop view cat.test_sudeep.v1")
    //
    //        spark.sql("create database cat.customdb")
    //        spark.sql("create table cat.customdb.tcustom(id int) using custom")
    //    df1.write.insertInto("cat.customdb.tcustom")
    //        df2.write.mode(SaveMode.Append).insertInto("cat.customdb.tcustom")
    //        spark.sql("insert into cat.customdb.tcustom values (9)")
    //        spark.read.table("cat.customdb.tcustom").show()


    //        spark.sql("create database cat.dbx115")
    //        spark.sql("create table cat.dbx115.tpart3(id int, name string, age int) using csv PARTITIONED BY (age) location '/tmp/tp'")
    //        spark.read.table("cat.dbx115.tpart3").show()

    //    val partitionDirs = DiscoverCatalogPartition.listPartitionDirRecurse("/tmp/tp").toSeq
    //    partitionDirs
    //
    //    val columnSpec = partitionDirs.map(pd => DiscoverCatalogPartition.detectPartitionFromSinglePath(pd.getHadoopPath, Set(new Path("file:///tmp/tp"))))
    //    columnSpec


    // df1.write.format("avro").mode("overwrite").saveAsTable("cat.test_sudeep.avr_tbl")


    // import org.apache.spark.sql.hive.util.DataFrameReaderExtension._
    //  spark.read.format("datahub").option("st","st").model("mod",df1)


    /** *datasource csv test case start** */
    // spark.sql("create database cat.dbx103");
    //spark.sql("create table cat.dbx103.tcsv(id string) using csv")
    //    df1.write.format("csv").mode("append").saveAsTable("cat.dbx103.tcsv")
    //    spark.conf.set("spark.insert.catalog","cat")
    //
    //    df1.write.mode(SaveMode.Append).insertInto("cat.dbx103.tcsv")
    //    spark.sql(""" INSERT INTO cat.dbx103.tcsv
    //                |     VALUES ('new_value')""".stripMargin)
    //    val df = spark.read.table("cat.dbx103.tcsv")
    //    val df2 = spark.sql("select * from cat.dbx103.tcsv")
    //    df.show()
    //    df2.show()
    /** datasource csv test case end* */


    /** *datasource parquet test case start** */
    // spark.sharedState.externalCatalog
    //  spark.sql("create database if not exists cat.dbx107");
    //    spark.sql("create table cat.dbx106.tparquet(id string) using parquet")
    //df1.write.format("csv").mode("append").saveAsTable("cat.dbx103.tcsv")
    //    spark.conf.set("spark.insert.catalog", "cat")
    //

    //    val df2 = spark.read.format("csv").option("header","false").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/dbx110.db/tcsv/part-00000-de8ac9df-4d60-4440-9454-6840f3fea1c9-c000.csv")
    //    df2.write.format("csv").saveAsTable("cat.dbx113.tt1")
    //    df2.write.format("parquet").saveAsTable("cat.dbx113.ttp")
    //   df2.write.format("delta").saveAsTable("cat.dbx116.ttd")

    /** **-----delta merge update and delete table issue ------*** */
    //    spark.sql("create database cat.dbx116")
    ////
    //    spark.sql("CREATE TABLE cat.dbx116.ttd (col1 String) USING delta")
    //    spark.sql(""" INSERT INTO cat.dbx116.ttd VALUES ('99'), ('6'), ('7')""".stripMargin)
    //    df2.write.insertInto("cat.dbx116.ttd")
    //    spark.sql("select * from cat.dbx116.ttd").show()
    //    spark.read.table("cat.dbx116.ttd").show()
    //    spark.sql("update cat.dbx116.ttd set col1 = 'six' where col1 = '6' ").show()
    //    spark.sql("select col1 from cat.dbx116.ttd").show()
    ////    spark.sql("select * from cat.dbx116.ttd").show()
    //    spark.sql("delete from cat.dbx116.ttd where col1 = '99'")
    //    spark.read.table("cat.dbx116.ttd").show()
    //    spark.sql("describe history cat.dbx116.ttd").show
    //    spark.sql("describe detail cat.dbx116.ttd").show
    //
    //     //   spark.read.table("cat.dbx116.ttd").show()
    // //   spark.sql("select * from cat.dbx116.ttd").show()
    //
    //    //
    //    //    df2.write.insertInto("cat.dbx113.ttp")
    //    //    spark.read.table("cat.dbx113.ttp").show()
    //    //
    //    /**---normal data source table operation ----**/
    //
    //
    //    spark.sql("create database cat.dbx119")
    //    spark.sql("create table cat.dbx119.ttp(c1 int, c2 int) using parquet")
    //    spark.sql("""insert into cat.dbx119.ttp values (1,11), (2,22), (3,33)""")
    //    spark.sql("select * from cat.dbx119.ttp where c1>1").show
    //
    //    spark.sql("create view cat.dbx119.v(id, id11) as select * from  cat.dbx119.ttp")
    //    spark.sql("select * from cat.dbx119.v").show()
    //    spark.sql("select c1, c2, fadd('', c1, c2) as c3 from cat.dbx119.ttp").show
    //    spark.sql("""create table cat.dbx119.ttd(c1 int, c2 int) using delta cluster by(c1)""")


    /** ---end of normal data source table operation ----* */

    /*delta merge*/
    //    spark.sql("create database cat.dbx120")
    //    spark.sql("create table cat.dbx120.ttp(c1 int, c2 int) using parquet")
    //    spark.sql("""insert into cat.dbx120.ttp values (1,11), (2,22), (3,33)""")
    //    spark.sql("create table cat.dbx120.ttd(c1 int, c2 int) using delta")
    //    spark.sql("""insert into cat.dbx120.ttd values (1,111), (4,44), (33,33)""")
    //    spark.sql(
    //      """merge into cat.dbx120.ttd using cat.dbx120.ttp on cat.dbx120.ttd.c1 = cat.dbx120.ttp.c1
    //        |WHEN MATCHED THEN UPDATE SET
    //        |c1 = cat.dbx120.ttp.c1,
    //        |c2 = cat.dbx120.ttp.c2
    //        |WHEN NOT MATCHED
    //        |  THEN INSERT (
    //        |  c1,
    //        |  c2)
    //        |  values(
    //        |  cat.dbx120.ttp.c1,
    //        |  cat.dbx120.ttp.c2)""".stripMargin)
    //    spark.read.table("cat.dbx120.ttd").show()
    //    spark.sql("describe history cat.dbx120.ttd").show()


    /** *Codegen for custom functions path ** */
    //    spark.sql("create database if not exists cat.dbx121")
    //    df1.write.format("orc").saveAsTable("cat.dbx121.tbsv")
    //    spark.read.table("cat.dbx121.tbsv").show()
    //    spark.sql("create table cat.dbx121.ttp1(c1 int, c2 String, c3 int) using parquet")
    //    spark.sql("""insert into cat.dbx121.ttp1 values (1,'hello', 11), (2,'hi',22), (3,'bye',33)""")
    ////    spark.sql("select fadd(c1,c3) from cat.dbx121.ttp1 ").show()
    //    spark.sql("select c1, c2, fibo(c1) as c3 from cat.dbx121.ttp1").show
    //    spark.sql("select fibo(4) as c4").show
    /** *Codegen for custom functions path ** */


    //
    //        spark.sql("create database cat.dbx112")
    //      //  val df2 = spark.read.format("csv").option("header", "false").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/dbx110.db/tcsv/part-00000-de8ac9df-4d60-4440-9454-6840f3fea1c9-c000.csv")
    //        //df2.write.format("csv").mode(SaveMode.Overwrite).saveAsTable ("cat.dbx112.tt1")
    //        df2.write.format("csv").mode(SaveMode.Append).saveAsTable ("cat.dbx112.tt1")
    //        df2.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("cat.dbx112.ttp")
    //    //    df2.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("cat.dbx112.ttp")
    //        df2.write.format("delta").mode(SaveMode.Overwrite)saveAsTable("cat.dbx112.ttd")
    //
    //     spark.sql("create database cat.dbx107")
    //        spark.sql("create table cat.dbx107.tt(id int) using delta")
    //
    //        spark.sql(
    //          """ INSERT INTO cat.dbx107.tt
    //            |     VALUES (1), (2), (3)""".stripMargin)


    //   spark.sql("create view cat.dbx107.v(id) as select * from cat.dbx107.tt")

    //   spark.sql("select * from cat.dbx107.v").show()

    //    df1.write.mode(SaveMode.Append).insertInto("cat.dbx106.tparquet")
    //    spark.sql(
    //      """ INSERT INTO cat.dbx106.tparquet
    //        |     VALUES ('new_value')""".stripMargin)
    //    val df = spark.read.table("cat.dbx106.tparquet")
    //    val df2 = spark.sql("select * from cat.dbx106.tparquet")
    //    df.show()
    //    df2.show()
    //    df2.write.format("orc").mode("append").saveAsTable("cat.dbx106.to")

    //    spark.sql("create database cat.dbx110")
    //    spark.sql("create table cat.dbx110.tcsv(id string, name String, age int) using csv")
    //    spark.sql(
    //            """ INSERT INTO cat.dbx110.tcsv
    //              |     VALUES ('id1', 'sharad', 30), ('id2', 'Xiaoyu', 32), ('id3', 'Bharath', 29), ('id4', 'Shiv', 32)""".stripMargin)
    //    spark.sql("""select age, name from cat.dbx110.tcsv""").show()


    /** datasource parquet test case end* */

    /** *datasource delta test case start** */
    //    spark.sql("create database cat.dbx106");
    //    spark.sql("create table cat.dbx106.ttex(id int) using custom")
    //    spark.sql("select * from  cat.dbx106.ttex").show()
    //    spark.read.table("cat.dbx106.ttex").show()
    //
    ////        spark.sql(
    ////              """ INSERT OVERWRITE TABLE cat.dbx106.ttex
    //  //              |     VALUES ('Sunny')""".stripMargin)
    //    spark.sql(
    //          """ INSERT INTO cat.dbx106.ttex
    //            |     VALUES (1)""".stripMargin)
    //    df1.write.mode(SaveMode.Append).insertInto("cat.dbx106.ttex")

    //  df1.write.mode(SaveMode.Overwrite).insertInto("cat.dbx106.ttex")

    //    spark.sql("create table cat.dbx105.tdelta(id string) using delta")
    //    //df1.write.format("csv").mode("append").saveAsTable("cat.dbx103.tcsv")
    //    spark.conf.set("spark.insert.catalog", "cat")
    //
    //    df1.write.mode(SaveMode.Append).insertInto("cat.dbx105.tdelta")
    //    spark.sql(
    //      """ INSERT INTO cat.dbx105.tdelta
    //        |     VALUES ('new_value')""".stripMargin)
    //    val df3 = spark.read.table("cat.dbx105.tdelta")
    //    val df4 = spark.sql("select * from cat.dbx105.tdelta")
    //    df3.show()
    //    df4.show()
    //    df4.write.format("parquet").mode("append").saveAsTable("cat.dbx105.tp")
    /** datasource delta test case ends* */


    /** Hive Relation testing start * */
    //   spark.sql("create database cat.dbx108");
    //  spark.conf.set("spark.insert.catalog","cat")
    //        df1.write.mode(SaveMode.Append).insertInto("cat.dbx108.hive_tbl")
    //    //    spark.sql(""" INSERT INTO cat.dbx103.tcsv
    //
    //        spark.sql("CREATE TABLE cat.dbx108.hive_tbl (col1 String) USING hive OPTIONS ('fileformat'='csv')")
    //        spark.sql(""" INSERT INTO cat.dbx108.hive_tbl VALUES ('new_value')""".stripMargin)
    //        val df3 = spark.sql("select * from cat.dbx108.hive_tbl")
    //        val df4 = spark.read.table("cat.dbx108.hive_tbl")
    //        df3.show()
    //        df4.show()
    //        spark.sql("""create view cat.dbx108.v1(c) as select * from cat.dbx108.hive_tbl""")
    //        val df5 = spark.read.table("cat.dbx108.v1")
    //        df5.show()
    //
    //    spark.sql("CREATE TABLE cat.dbx108.csv_tbl (col1 String) USING csv")
    //    spark.sql(""" INSERT INTO cat.dbx108.csv_tbl VALUES ('new_value')""".stripMargin)
    //    spark.sql("select * from cat.dbx108.csv_tbl").show()
    //    spark.read.table("cat.dbx108.csv_tbl").show()
    //
    //    spark.sql("""create view cat.dbx108.v2(c) as select * from cat.dbx108.csv_tbl""")
    //    spark.read.table("cat.dbx108.v2").show()
    //    spark.sql("select * from cat.dbx108.v2").show()
    //
    //    spark.sql("CREATE TABLE cat.dbx108.delta_tbl (col1 String) USING delta")
    //    spark.sql(""" INSERT INTO cat.dbx108.delta_tbl VALUES ('new_value')""".stripMargin)
    //    spark.sql("select * from cat.dbx108.delta_tbl").show()
    //    spark.read.table("cat.dbx108.delta_tbl").show()
    //
    //
    //    spark.sql("""create view cat.dbx108.v3(c) as select * from cat.dbx108.delta_tbl""")
    //    spark.read.table("cat.dbx108.v3").show()
    //    spark.sql("select * from cat.dbx108.v3").show()

    /** Hive Relation testing end * */

    //    spark.sql("create database cat.dbx102")
    ////    spark.sql("CREATE TABLE cat.dbx101.delta_tb (col1 String) USING delta")
    //    df1.write.format("delta").mode("append").saveAsTable("cat.dbx102.delta_tb")
    //   val df = spark.sql("select * from cat.dbx101.delta_tb")
    //   df.show()
    //    spark.sql("ALTER TABLE cat.dbx101.delta_tb ADD columns (LastName string, DOB timestamp)")
    //   spark.sql("create database cat.dbx97z")
    //    spark.sql("CREATE TABLE cat.dbx97z.hive_ext (col1 String) USING hive OPTIONS ('fileformat'='orc') location '/tmp/p1'")
    //    val df = spark.sql("select * from cat.dbx97.hive_ext")
    //    df.show()
    //    spark.sql("create table cat.dbx83.tt(id int, name string) using csv")
    //
    //   // spark.sql("create database cat.dbx81")
    //    spark.sql("create table cat.dbx83.ttex(id int) using custom")
    //    //spark.sql("create table cat.dbx28.ttp(id int) using parquet")
    //    //val df1 = spark.read.table("cat.dbx68.tt")
    //   // df1.show()
    //    val df = spark.sql("select name from cat.dbx83.tt")
    //    val df2 = spark.sql("select * from cat.dbx83.ttex")
    //    df.show()
    //    df2.show()
    // df1.write.format("parquet").save()

    //    df1.write.format("csv").saveAsTable("cat.dbx66.tt1")

    //    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000L),
    //      ("Michael ", "Rose", "", 2010, 3, "M", 4000L),
    //      ("Robert ", "", "Williams", 2010, 3, "M", 4000L),
    //      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000L),
    //      ("Jen", "Mary", "Brown", 2010, 7, "", 2000L)
    //    )
    //    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
    //      "dob_month", "gender", "salary")
    //
    //    val dfLocal = data.toDF(columns: _*)
    //    dfLocal.write.format("csv").saveAsTable("default.tb13")
    //    dfLocal.show()
    //    dfLocal.printSchema()

    //    spark.sql("""create table if not exists tbl_csv(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using csv options (header=true) location '/tmp/csv/'""")
    //    dfLocal.write.insertInto("tbl_csv")
    //    spark.sql("generate deltalog for table default.tbl_csv using csv")


    //    //test for partition with both flavors of sql (location and table) generate delta log
    //    spark.sql("drop table if exists tbl_orc1")
    //    spark.sql("drop table if exists tbl_orc2")
    //    spark.sql("create table if not exists tbl_orc1(id string, name string) using orc partitioned by(name) " )
    //    spark.sql("create table if not exists tbl_orc2(id string , name string ) using orc partitioned by(name) " )
    //    spark.sql("""insert into tbl_orc1 values("1", "Xiaoyu"), ("2", "Bharat"), ("3", "Vivek"),("4", "Sharad") """)
    //    spark.sql("""insert into tbl_orc2 values("1", "Xiaoyu"), ("2", "Bharat"), ("3", "Vivek"),("4", "Sharad") """)
    //    val path = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl_orc2")).storage.locationUri.get.getPath
    ////    spark.sql("generate deltalog for table default.tbl_orc1 using orc")
    //    spark.sql(s"generate deltalog for location '${path}' using orc")


    //
    //    spark.sql("""create table if not exists tbl_orc(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using csv options (header=true) location '/tmp/csv/'""")
    //    dfLocal.write.insertInto("tbl_orc")
    //    spark.sql("generate deltalog for table default.tbl_csv using orc")
    //
    //    spark.sql("""create table if not exists tbl_json(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using json location '/tmp/json/'""")
    //    dfLocal.write.insertInto("tbl_json")
    //    spark.sql("generate deltalog for table default.tbl_json using json")
    //
    //    spark.sql("""create table if not exists tbl_avro(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using avro  location '/tmp/avro/'""")
    //    dfLocal.write.insertInto("tbl_avro")
    //    spark.sql("generate deltalog for table default.tbl_avro using avro")
    //
    //    spark.sql("""create table if not exists tbl_parquet(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using parquet location '/tmp/parquet/'""")
    //    dfLocal.write.insertInto("tbl_parquet")
    //    spark.sql("generate deltalog for table default.tbl_parquet using parquet")
    //
    //    var dl = DeltaLog.forTable(spark,"/tmp/csv/")
    //    print(dl.snapshot.metadata)
    //
    //    dl = DeltaLog.forTable(spark,"/tmp/orc/")
    //    print(dl.snapshot.metadata)
    //
    //    dl = DeltaLog.forTable(spark,"/tmp/json/")
    //    print(dl.snapshot.metadata)
    //
    //    dl = DeltaLog.forTable(spark,"/tmp/avro/")
    //    print(dl.snapshot.metadata)
    //
    //    dl = DeltaLog.forTable(spark,"/tmp/parquet/")
    //    print(dl.snapshot.metadata)

    // Tests from Bharat
    //    val data = Seq(("1", "ssh"), ("2 ", "xy"), ("3 ", "sh"), ("4 ", "bh"))
    //    val columns = Seq("id", "name")
    //    val dfLocal = data.toDF(columns: _*)
    //    dfLocal.show()
    //    dfLocal.printSchema()
    //
    //    spark.sql("""create table if not exists tbl_csv(id string, name string) using csv options (header=true) location '/tmp/copy-csv/'""")
    //    dfLocal.write.insertInto("tbl_csv")
    //    spark.sql("select * from tbl_csv")
    //
    // spark.sql("""create table if not exists default.copy_tbl_parquet99(id string, name String) using parquet """)
    //  spark.sql("copy into default.copy_tbl_parquet99 from '/tmp/copy-csv' fileformat = csv format_options('header'='true', 'delimiter'=',')")
    //    var dfRead = spark.read.table("default.copy_tbl_parquet1")
    //    dfRead.show()
    //
    //    spark.sql("""create table if not exists default.copy_tbl_parquet1_without_options(id string, name String) using parquet """)
    //    spark.sql("copy into default.copy_tbl_parquet1_without_options from '/tmp/copy-csv' fileformat = csv")
    //    dfRead = spark.read.table("default.copy_tbl_parquet1_without_options")
    //    dfRead.show()

    //spark.sql("""create table if not exists t_delta(id string, name String) using delta """)

    //spark.sql("copy into default.t_delta from '/Users/shabaner/databricks-copy-into/src/main/resources/data' fileformat = csv files=('1.csv', '2.csv')")
    //spark.sql("COPY into default.t_delta from '/Users/shabaner/databricks-copy-into/src/main/resources/data' fileformat = csv pattern = '*.csv'")
    //  spark.sql("copy into default.t_delta from { select * from '/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data/' } fileformat = csv files=('1.csv', '2.csv')" )


  }
}
