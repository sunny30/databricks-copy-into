import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.hive.datashare.ConverterUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object App {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.sql.extensions", "org.apache.spark.sql.hive.CustomExtensionSuite")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("hive.exec.dynamic.partition.mode" , "nonstrict")
  }

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("spark-3.5.1-lake").master("local").
      config(getConf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000L),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000L),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000L),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000L),
      ("Jen", "Mary", "Brown", 2010, 7, "", 2000L)
    )
    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")

    val dfLocal = data.toDF(columns: _*)
    dfLocal.show()
    dfLocal.printSchema()

//    spark.sql("""create table if not exists tbl_csv(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using csv options (header=true) location '/tmp/csv/'""")
//    dfLocal.write.insertInto("tbl_csv")
//    spark.sql("generate deltalog for table default.tbl_csv using csv")
//
//
//
////    //test for partition with both flavors of sql (location and table) generate delta log
//    spark.sql("drop table if exists tbl_orc1")
//    spark.sql("drop table if exists tbl_orc2")
//    spark.sql("create table if not exists tbl_orc1(id string, name string) using orc partitioned by(name) " )
//    spark.sql("create table if not exists tbl_orc2(id string , name string ) using orc partitioned by(name) " )
//    spark.sql("""insert into tbl_orc1 values("1", "Xiaoyu"), ("2", "Bharat"), ("3", "Vivek"),("4", "Sharad") """)
//    spark.sql("""insert into tbl_orc2 values("1", "Xiaoyu"), ("2", "Bharat"), ("3", "Vivek"),("4", "Sharad") """)
//    val path = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl_orc2")).storage.locationUri.get.getPath
//    spark.sql("generate deltalog for table default.tbl_orc1 using orc")
   // spark.sql(s"generate deltalog for location '${path}' using orc")


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
//    spark.sql("""create table if not exists default.copy_tbl_parquet1(id string, name String) using parquet """)
//    spark.sql("copy into default.copy_tbl_parquet1 from '/tmp/copy-csv' fileformat = csv format_options('header'='true', 'delimiter'=',')")
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
   // spark.sql("copy into default.t_delta from { select * from '/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data/' } fileformat = csv files=('1.csv', '2.csv')" )


  }
}
