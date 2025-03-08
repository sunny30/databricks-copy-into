import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object VanilaSparkApp {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("vanila-app").master("local").
      config(getConf).
      enableHiveSupport().
      getOrCreate()

    spark.sql("create table vt1(id int) using delta")
    spark.sql("insert into vt1 values(1), (2)")
    spark.sql("select * from vt1 as of version 0")
  }

}
