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
        |CREATE TABLE if not exists hudi_table7(
        |    id long,
        |    city STRING
        |) USING HUDI
        |PARTITIONED BY (city);
        |""".stripMargin)


    spark.sql("""insert into hudi_table7 values (1, "mum"),(2, "mum") , (4, "bng"), (5, "bng"),(6, "bng") , (3, "mum")""")
    spark.sql("select id from hudi_table7 where city = 'bng'").show


  }

}
