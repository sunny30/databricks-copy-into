import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HudiApp {

  def main(args:Array[String]):Unit={

    val sparkConf = new SparkConf().setAppName("Example Spark App").setMaster("local[*]").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar").
      set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").
      set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

    val spark = SparkSession.builder.appName("Hudi App").config(sparkConf).getOrCreate

    spark.sql(
      """
        |CREATE TABLE hudi_table5 (
        |    ts BIGINT,
        |    uuid STRING,
        |    rider STRING,
        |    driver STRING,
        |    fare DOUBLE,
        |    city STRING
        |) USING HUDI
        |PARTITIONED BY (city);
        |""".stripMargin)


  }

}
