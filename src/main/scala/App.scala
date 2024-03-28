import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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

    spark.sql("""create table if not exists tbl_csv(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using csv options (header=true) location '/tmp/csv/'""")
    dfLocal.write.insertInto("tbl_csv")
    spark.sql("generate deltalog for table default.tbl_csv using csv")

    spark.sql("""create table if not exists tbl_orc(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using csv options (header=true) location '/tmp/csv/'""")
    dfLocal.write.insertInto("tbl_orc")
    spark.sql("generate deltalog for table default.tbl_csv using orc")

    spark.sql("""create table if not exists tbl_json(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using json location '/tmp/json/'""")
    dfLocal.write.insertInto("tbl_json")
    spark.sql("generate deltalog for table default.tbl_json using json")

    spark.sql("""create table if not exists tbl_avro(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using avro  location '/tmp/avro/'""")
    dfLocal.write.insertInto("tbl_avro")
    spark.sql("generate deltalog for table default.tbl_avro using avro")

    spark.sql("""create table if not exists tbl_parquet(firstname string, middlename string, lastname string, dob_year int, dob_month int, gender string, salary long) using parquet location '/tmp/parquet/'""")
    dfLocal.write.insertInto("tbl_parquet")
    spark.sql("generate deltalog for table default.tbl_parquet using parquet")


    var dl = DeltaLog.forTable(spark,"/tmp/csv/")
    print(dl.snapshot.metadata)

    dl = DeltaLog.forTable(spark,"/tmp/orc/")
    print(dl.snapshot.metadata)

    dl = DeltaLog.forTable(spark,"/tmp/json/")
    print(dl.snapshot.metadata)

    dl = DeltaLog.forTable(spark,"/tmp/avro/")
    print(dl.snapshot.metadata)

    dl = DeltaLog.forTable(spark,"/tmp/parquet/")
    print(dl.snapshot.metadata)

  }
}
