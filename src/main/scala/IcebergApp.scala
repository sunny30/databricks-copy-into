import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IcebergApp {

  def main(args:Array[String]):Unit={

    val sparkConf = new SparkConf().setAppName("Example Spark App").setMaster("local[*]").
      set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
      set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog").
//      set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog").
      set("spark.sql.catalog.local.type", "hadoop").
      set("spark.sql.catalog.local.warehouse", "/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/local")
    val spark = SparkSession.builder.appName("Example Spark App").config(sparkConf).getOrCreate

    spark.sql("""CREATE TABLE local.db.logs (
                |    uuid string NOT NULL,
                |    level string NOT NULL)
                |USING iceberg""".stripMargin)

    //org.apache.iceberg.spark.source.IcebergSource
    spark.sql("insert into local.db.logs values('a', 'b')")

   // spark.sql("describe formatted local.db.logs").show()

   // val df = spark.sql("select * from local.db.logs")

  //  df.show()

    spark.sql("select * from local.db.logs").show()

    spark.read.table("local.db.logs").show()

//    spark.sql("""create table local.db.logs_orc(
//      | uuid string NOT NULL,
//      | level string NOT NULL)
//    | USING orc""".stripMargin)
//
//    spark.sql("insert into local.db.logs_orc values('a', 'b')")
    spark.sql(
      """create table if not exists logs_orc7(
        | uuid string NOT NULL,
        | level string NOT NULL)
        | USING orc""".stripMargin)

    spark.sql("insert into logs_orc7 values('a', 'b')")
    spark.sql("CALL local.system.migrate('spark_catalog.default.logs_orc7')")
  //  df.write.format("iceberg").saveAsTable("local.db.new_logs")

  //  spark.sql("select * from local.db.logs").explain(true)


  }

}

