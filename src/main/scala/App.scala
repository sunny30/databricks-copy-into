import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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


    val data = Seq(("1", "ssh"), ("2 ", "xy"), ("3 ", "sh"), ("4 ", "bh"))
    val columns = Seq("id", "name")
    val dfLocal = data.toDF(columns: _*)
    dfLocal.show()
    dfLocal.printSchema()

    spark.sql("""create table if not exists tbl_csv(id string, name string) using csv options (header=true) location '/tmp/copy-csv/'""")
    dfLocal.write.insertInto("tbl_csv")
    spark.sql("select * from tbl_csv")

    spark.sql("""create table if not exists default.t_delta2(id string, name String) using delta """)
    spark.sql("copy into default.t_delta2 from '/tmp/copy-csv' fileformat = csv")
    val dfRead = spark.read.table("default.t_delta2")
    dfRead.show()


  }
}
