import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive.catalog.HMSCatalog
import org.apache.spark.sql.types.StructType

object VanilaSparkApp {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]").set("spark.sql.hive.metastore.version", "3.1.3")
//      set("spark.sql.hive.metastore.jars", "path").
//      set("spark.sql.test.env", "true").
//      set("spark.sql.hive.metastore.jars.path",
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/jackson-core-2.6.7.jar" +
//          "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/jackson-databind-2.6.7.3" +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/parquet-column-1.13.1.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-metastore-3.1.3.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-exec-3.1.3.jar, " +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-logging-1.1.1.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-io-2.7.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/calcite-core-1.32.0.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/thrift-1.0.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/libfb303-0.9.3.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/dropwizard-core-2.1.5.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/metrics-core-3.0.2.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/hive-common-3.1.3.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-core-4.1.17.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-rdbms-4.1.17.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/datanucleus-api-jdo-4.2.4.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/HikariCP-2.5.1.jar," +
//        "/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/derby-10.14.2.0.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/javax.jdo-3.2.0-release.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/commons-collections-3.2.2.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/antlr-runtime-3.5.3.jar," +
//        "file:///Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/servlet-api-2.3.jar")

      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("vanila-app").master("local").
      config(getConf).
     // enableHiveSupport().
      getOrCreate()

    import spark.implicits._

    val df3 = Seq(
      (7, "John", 2.0),
      (8, "Sunny", 3.0),
      (9, "Xiaoyu", 4.0),
      (10, "Shashi", 5.0),
      (11, "Bharath", 6.0),
      (12, "Vivek", 7.0)
    ).toDF("col1", "col2", "col3")

   // spark.sql("create table save_tbl1(id int) using parquet")
    df3.write.format("parquet").mode(SaveMode.Overwrite).option("path", "/tmp/vtbl").saveAsTable("save_tbl9")
    df3.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("save_tbl9")
    spark.sql("describe formatted save_tbl9").show()

////    spark.sql("create table vt1(id int) using delta")
////    spark.sql("insert into vt1 values(1), (2)")
////    spark.sql("select * from vt1 as of version 0")
//   // spark.read.format("avro").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/lidb.db/ptbl_BACKUP_/metadata/1609def0-56c8-4b44-83da-a5245ed9dbbf-m0.avro").show(truncate = false)
//
//    val dt = spark.sessionState.sqlParser.parseDataType("""STRUCT<
//                                                 |    `1%`: DOUBLE,
//                                                 |    `5%`: DOUBLE,
//                                                 |    `10%`: DOUBLE,
//                                                 |    `25%`: DOUBLE,
//                                                 |    `50%`: DOUBLE,
//                                                 |    `75%`: DOUBLE,
//                                                 |    `90%`: DOUBLE,
//                                                 |    `95%`: DOUBLE,
//                                                 |    `99%`: DOUBLE,
//                                                 |    `count`: DOUBLE,
//                                                 |    `max`: DOUBLE,
//                                                 |    `mean`: DOUBLE,
//                                                 |    `min`: DOUBLE,
//                                                 |    `std`: DOUBLE
//                                                 |  >""".stripMargin)
////    val s = """STRUCT<
////                 |    1%: DOUBLE,
////                 |    5%: DOUBLE,
////                 |    10%: DOUBLE,
////                 |    `25%`: DOUBLE,
////                 |    `50%`: DOUBLE,
////                 |    `75%`: DOUBLE,
////                 |    `90%`: DOUBLE,
////                 |    `95%`: DOUBLE,
////                 |    `99%`: DOUBLE,
////                 |    `count`: DOUBLE,
////                 |    `max`: DOUBLE,
////                 |    `mean`: DOUBLE,
////                 |    `min`: DOUBLE,
////                 |    `std`: DOUBLE
////                 |  >""".stripMargin
////
////    val s2= """ARRAY<STRUCT<
////              |        street: STRING,
////              |        city: STRING,
////              |        zip% code: INT,
////              |        is primary%: BOOLEAN,
////              |        rem string: STRING
////              |    >>""".stripMargin
////
////
////
////    val quotedStruct = s.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
////    val dt1 = spark.sessionState.sqlParser.parseDataType(quotedStruct)
////    println(dt1.sql)
////
////    val quotedStruct1 = s2.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
////    val dt2 = spark.sessionState.sqlParser.parseDataType(quotedStruct1)
////    println(dt2.sql)
////
////    val s3 = """STRUCT<
////               |        personal: STRUCT<
////               |            ssn%last_four%: INT,
////               |            birth_date: DATE
////               |        >,
////               |        verification: STRUCT<
////               |            status: STRING,
////               |            last_checked%: TIMESTAMP
////               |        >
////               |    >""".stripMargin
////
////
////    val quotedStruct2 = s3.replaceAll("""(?<=[,<]|^)\s*([^,<>:`]+?)(?=\s*:)""", "`$1`")
////    val dt3 = spark.sessionState.sqlParser.parseDataType(quotedStruct2)
////    println(dt3.sql)
////
////    val fs = new FieldSchema("id",s, "" )
////    val sdt = (new HMSCatalog("",spark.sparkContext.getConf,null,spark)).getSparkSQLDataType(fs)
////    println(sdt.sql)
////
////    val fs1 = new FieldSchema("id", s2, "")
////    val sdt1 = (new HMSCatalog("", spark.sparkContext.getConf, null, spark)).getSparkSQLDataType(fs1)
////    println(sdt1.sql)
////
////    val fs2 = new FieldSchema("id", s3, "")
////    val sdt2 = (new HMSCatalog("", spark.sparkContext.getConf, null, spark)).getSparkSQLDataType(fs2)
////    println(sdt2.sql)
////
//
//    spark.sql("create table if not exists ptid(id int) using parquet")
//    spark.sql("select * from ptid").explain(true)
//
//    spark.sql("create table if not exists phtid(id int) using hive options('fileformat' = 'parquet')")
//    spark.sql("select * from phtid").explain(true)
    // val schema = StructType.fromDDL(s)
   // println(schema.prettyJson)
  //  println(dt.sql)
    //spark.read.format("avro").load("/Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/local/db/log_orc_snap/metadata/8a82f4c6-fe2b-494c-b107-b065ee08313e-m0.avro").show(truncate = false)
  }

}
