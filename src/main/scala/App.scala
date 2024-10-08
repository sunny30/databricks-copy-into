import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.hive.datashare.ConverterUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object App {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]").
      set("spark.sql.hive.metastore.version", "3.1.3").
      set("spark.sql.hive.metastore.jars", "path").
      set("spark.sql.test.env", "true").
      set("spark.sql.hive.metastore.jars.path", "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/parquet-column-1.13.1.jar,"+
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
      .set("spark.sql.catalog.cat", "org.apache.spark.sql.hive.catalog.UnityCatalog").set("parquet.compression", "SNAPPY")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.5.1-lake").master("local").
      config(getConf).
      enableHiveSupport().
      getOrCreate()

    import spark.implicits._


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
          1,
          2
        ).toDF("col1")


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
    spark.conf.set("spark.insert.catalog", "cat")



   // spark.sql("create database cat.dbx107")
  //    spark.sql("create table cat.dbx107.tt(id int) using delta")

//    spark.sql(
//      """ INSERT INTO cat.dbx107.tt
//        |     VALUES (1), (2), (3)""".stripMargin)



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

    /** datasource parquet test case end* */

    /** *datasource delta test case start** */
    spark.sql("create database cat.dbx106");
    spark.sql("create table cat.dbx106.ttex(id int) using custom")
    spark.sql("select * from  cat.dbx106.ttex").show()
    spark.read.table("cat.dbx106.ttex").show()

//        spark.sql(
//              """ INSERT OVERWRITE TABLE cat.dbx106.ttex
  //              |     VALUES ('Sunny')""".stripMargin)
    spark.sql(
          """ INSERT INTO cat.dbx106.ttex
            |     VALUES (1)""".stripMargin)
    df1.write.mode(SaveMode.Append).insertInto("cat.dbx106.ttex")

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
//    spark.sql("create database cat.dbx102");
//    spark.sql("CREATE TABLE cat.dbx102.hive_tbl (col1 String) USING hive OPTIONS ('fileformat'='csv')")
//    spark.conf.set("spark.insert.catalog","cat")
//    df1.write.mode(SaveMode.Append).insertInto("cat.dbx102.hive_tbl")
//    //    spark.sql(""" INSERT INTO cat.dbx103.tcsv
//
//    spark.sql(""" INSERT INTO cat.dbx102.hive_tbl VALUES ('new_value')""".stripMargin)
//    val df3 = spark.sql("select * from cat.dbx102.hive_tbl")
//    val df4 = spark.read.table("cat.dbx102.hive_tbl")
//    df3.show()
//    df4.show()
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
    //df1.write.format("csv").saveAsTable("cat.dbx66.tt1")

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
