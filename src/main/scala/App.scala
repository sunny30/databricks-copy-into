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
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser

object App {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]").
      set("spark.sql.hive.metastore.version", "3.1.3").
      set("spark.sql.hive.metastore.jars", "path").
      set("spark.sql.test.env", "true").
      set("spark.sql.hive.metastore.jars.path", "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/parquet-column-1.13.1.jar," +
        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-metastore-3.1.3.jar," +
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
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.5.1-lake").master("local").
      config(getConf).
      enableHiveSupport().
      getOrCreate()

    import spark.implicits._
   // import spark.sqlContext.implicits._


   // val hpl = CustomSparkSQLParser.parsePlan("describe history cat.db.tb")


  //  val cbpl = CustomSparkSQLParser.parsePlan("create table cat.db.tb(id int, name string) using delta cluster by(id)")

  //  cbpl


    // spark.sql("create database lsdb2")
    //spark.sql("set spark.sql.parquet.compression.codec=lz4raw")
    //    val df1 = Seq(
    //      "John",
    //      "Sunny",
    //      "Xiaoyu",
    //      "Shashi",
    //      "Bharath",
    //      "Vivek"
    //    ).toDF("col1")


    val df1 = Seq(
      (1,2),
      (2,3)
    ).toDF("col1", "col2")

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
//        spark.read.table("cat.test_sudeep.jsonn_tbl").show()
//        spark.sql("select * from cat.test_sudeep.jsonn_tbl").show()

        val df2 = Seq(
          6,
          7
        ).toDF("col1")
//    df1.write.format("delta").saveAsTable("cat.dbx117.delta_tbl")
//    spark.sql("describe formatted cat.dbx117.delta_tbl").show()
//    spark.sql("describe formatted cat.dbx117.json_tbl").show()
        //df2.write.format("parquet").mode("overwrite").insertInto("cat.test_sudeep.jsonn_tbl")
//        spark.read.table("cat.test_sudeep.jsonn_tbl").show()
//        spark.sql("select * from cat.test_sudeep.jsonn_tbl").show()
//        spark.sql("insert into cat.test_sudeep.jsonn_tbl values (5), (4)")
//        spark.sql("select * from cat.test_sudeep.jsonn_tbl where col1>4").show()
//
//
//        spark.sql("create view cat.test_sudeep.v1(c) as select * from cat.test_sudeep.jsonn_tbl where col1>4")
//
//
//        spark.sql("select * from cat.test_sudeep.v1").show()
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

    /****-----delta merge update and delete table issue ------****/
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


    /**---end of normal data source table operation ----**/

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


    /***Codegen for custom functions path ***/
    spark.sql("create database if not exists cat.dbx121")
//    df1.write.format("orc").saveAsTable("cat.dbx121.tbsv")
//    spark.read.table("cat.dbx121.tbsv").show()
    spark.sql("create table cat.dbx121.ttp1(c1 int, c2 String, c3 int) using parquet")
    spark.sql("""insert into cat.dbx121.ttp1 values (1,'hello', 11), (2,'hi',22), (3,'bye',33)""")
   // spark.sql("select fadd(c1,c3) from cat.dbx121.ttp1 ").show()
    spark.sql("select c1, c2, fiboinline(c1) as c3 from cat.dbx121.ttp1").show
  //  spark.sql("select fibo(4) as c4").show
    /***Codegen for custom functions path ***/



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
    //    spark.sql("create database cat.dbx108");
    //  spark.conf.set("spark.insert.catalog","cat")
    //    df1.write.mode(SaveMode.Append).insertInto("cat.dbx108.hive_tbl")
    //    //    spark.sql(""" INSERT INTO cat.dbx103.tcsv
    //
    //    spark.sql("CREATE TABLE cat.dbx108.hive_tbl (col1 String) USING hive OPTIONS ('fileformat'='csv')")
    //    spark.sql(""" INSERT INTO cat.dbx108.hive_tbl VALUES ('new_value')""".stripMargin)
    //    val df3 = spark.sql("select * from cat.dbx108.hive_tbl")
    //    val df4 = spark.read.table("cat.dbx108.hive_tbl")
    //    df3.show()
    //    df4.show()
    //    spark.sql("""create view cat.dbx108.v1(c) as select * from cat.dbx108.hive_tbl""")
    //    val df5 = spark.read.table("cat.dbx108.v1")
    //    df5.show()
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
