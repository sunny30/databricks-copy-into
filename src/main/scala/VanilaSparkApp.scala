import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.catalog.HMSCatalog
import org.apache.spark.sql.types.StructType

object VanilaSparkApp {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("vanila-app").master("local").
//      config(getConf).
//      enableHiveSupport().
      getOrCreate()

//    spark.sql("create table vt1(id int) using delta")
//    spark.sql("insert into vt1 values(1), (2)")
//    spark.sql("select * from vt1 as of version 0")
   // spark.read.format("avro").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/lidb.db/ptbl_BACKUP_/metadata/1609def0-56c8-4b44-83da-a5245ed9dbbf-m0.avro").show(truncate = false)

    val dt = spark.sessionState.sqlParser.parseDataType("""STRUCT<
                                                 |    `1%`: DOUBLE,
                                                 |    `5%`: DOUBLE,
                                                 |    `10%`: DOUBLE,
                                                 |    `25%`: DOUBLE,
                                                 |    `50%`: DOUBLE,
                                                 |    `75%`: DOUBLE,
                                                 |    `90%`: DOUBLE,
                                                 |    `95%`: DOUBLE,
                                                 |    `99%`: DOUBLE,
                                                 |    `count`: DOUBLE,
                                                 |    `max`: DOUBLE,
                                                 |    `mean`: DOUBLE,
                                                 |    `min`: DOUBLE,
                                                 |    `std`: DOUBLE
                                                 |  >""".stripMargin)
    val s = """STRUCT<
                 |    1%: DOUBLE,
                 |    5%: DOUBLE,
                 |    10%: DOUBLE,
                 |    `25%`: DOUBLE,
                 |    `50%`: DOUBLE,
                 |    `75%`: DOUBLE,
                 |    `90%`: DOUBLE,
                 |    `95%`: DOUBLE,
                 |    `99%`: DOUBLE,
                 |    `count`: DOUBLE,
                 |    `max`: DOUBLE,
                 |    `mean`: DOUBLE,
                 |    `min`: DOUBLE,
                 |    `std`: DOUBLE
                 |  >""".stripMargin

    val s2= """ARRAY<STRUCT<
              |        street: STRING,
              |        city: STRING,
              |        zip% code: INT,
              |        is primary%: BOOLEAN,
              |        rem string: STRING
              |    >>""".stripMargin



    val quotedStruct = s.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
    val dt1 = spark.sessionState.sqlParser.parseDataType(quotedStruct)
    println(dt1.sql)

    val quotedStruct1 = s2.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
    val dt2 = spark.sessionState.sqlParser.parseDataType(quotedStruct1)
    println(dt2.sql)

    val s3 = """STRUCT<
               |        personal: STRUCT<
               |            ssn%last_four%: INT,
               |            birth_date: DATE
               |        >,
               |        verification: STRUCT<
               |            status: STRING,
               |            last_checked%: TIMESTAMP
               |        >
               |    >""".stripMargin


    val quotedStruct2 = s3.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
    val dt3 = spark.sessionState.sqlParser.parseDataType(quotedStruct2)
    println(dt3.sql)

    val fs = new FieldSchema("id",s, "" )
    val sdt = (new HMSCatalog("",spark.sparkContext.getConf,null,spark)).getSparkSQLDataType(fs)
    println(sdt.sql)

    val fs1 = new FieldSchema("id", s2, "")
    val sdt1 = (new HMSCatalog("", spark.sparkContext.getConf, null, spark)).getSparkSQLDataType(fs1)
    println(sdt1.sql)


    // val schema = StructType.fromDDL(s)
   // println(schema.prettyJson)
  //  println(dt.sql)
    //spark.read.format("avro").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/local/db/log_orc_snap/metadata/8a82f4c6-fe2b-494c-b107-b065ee08313e-m0.avro").show(truncate = false)
  }

}
