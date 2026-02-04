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
                |    level string NOT NULL,
                |    age int,
                |    range int)
                |USING iceberg partitioned by(age, range)""".stripMargin)

    //org.apache.iceberg.spark.source.IcebergSource
    spark.sql("alter table local.db.logs add partition field bucket(2,age) as new_p")
    spark.sql("insert into local.db.logs values('a', 'b',1,2), ('a', 'b',2,2)")

    spark.sql("alter table local.db.logs add partition field bucket(2,level) as new_p1")

   // spark.sql("describe formatted local.db.logs").show()

   // val df = spark.sql("select * from local.db.logs")

  //  df.show()

    spark.sql("select * from local.db.logs").show()
    spark.sql("CALL local.system.create_changelog_view(table => 'db.logs',options => map('start-snapshot-id','1','end-snapshot-id', '2'))")

    spark.sql(
      """CREATE TABLE local.db.logs1 (
        |    uuid string NOT NULL,
        |    level string NOT NULL,
        |    age int,
        |    range int)
        |USING iceberg partitioned by (bucket(16,level))""".stripMargin)
    spark.sql("insert into local.db.logs1 values('a', 'b',1,2), ('a1', 'b1',2,2)")

  //  spark.read.table("local.db.logs").show()

//    spark.sql("""create table local.db.logs_orc(
//      | uuid string NOT NULL,
//      | level string NOT NULL)
//    | USING orc""".stripMargin)
//
//    spark.sql("insert into local.db.logs_orc values('a', 'b')")
//    spark.sql(
//      """create table if not exists logs_orc7(
//        | uuid string NOT NULL,
//        | level string NOT NULL)
//        | USING orc""".stripMargin)
//
//    spark.sql("insert into logs_orc7 values('a', 'b')")
//    spark.sql("CALL local.system.migrate('spark_catalog.default.logs_orc7')")
  //  df.write.format("iceberg").saveAsTable("local.db.new_logs")

  //  spark.sql("select * from local.db.logs").explain(true)


  }

}

