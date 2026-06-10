import io.delta.tables.hc.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object CLSApp {


  def viewCLSApp(spark: SparkSession):Unit = {

    spark.sql("""create database if not exists cat.cls_db2""")
    spark.sql("create table cat.cls_db2.ppt(id int, name string) using parquet PARTITIONED BY (name)")
   // spark.sql("describe table cat.cls_db2.ppt cls_id").show()
    //spark.sql("show columns in cat.cls_db2.ppt").show()
    spark.sql("insert into cat.cls_db2.ppt values(1,'sh'), (3, 'su')")
//    val cteQuery =
//      s"""WITH cte_data AS (SELECT * FROM cat.cls_db2.ppt)
//         |SELECT * FROM cte_data""".stripMargin
//    spark.sql(cteQuery).show()
   // spark.sql("SELECT * FROM cte_data").show()
   // spark.sql("truncate table cat.cls_db2.ppt")
   // spark.sql("select * from cat.cls_db2.ppt").show()

//    spark.sql(
//      """
//        |MERGE INTO cat.cls_db2.ppt AS target
//        |USING (
//        |SELECT * FROM VALUES
//        |    (1, 'Alice'),
//        |    (2, 'Bob')
//        |) AS source (id, name)
//        |ON target.id = source.id
//        |WHEN MATCHED THEN
//        |  UPDATE SET *
//        |WHEN NOT MATCHED THEN
//        |  INSERT *
//        |""".stripMargin)

  //  working merge
//    spark.sql(
//      """
//        |MERGE INTO cat.cls_db2.ppt AS target
//        |USING (SELECT 1 AS id, 'Alice' AS name, 100 AS amount) AS source
//        |ON target.id = source.id
//        |WHEN MATCHED THEN
//        |    UPDATE SET target.name = 'Alice'
//        |WHEN NOT MATCHED THEN
//        |    INSERT (id, name) VALUES (1, 'Alice');
//        |""".stripMargin)
   // spark.sql("update cat.cls_db2.ppt set id = 4 where name1 = 'sh'")
  //  spark.sql("select * from cat.cls_db2.ppt").show()
    spark.sql("create view cat.cls_db2.v1(cls_id , name) as select *  from cat.cls_db2.ppt")
    spark.sql("describe formatted cat.cls_db2.v1").show()
  //  spark.sql("show columns in cat.cls_db2.v1").show()
//    spark.sql("select * from cat.cls_db2.v1").show()
//    spark.sql("show columns in cat.cls_db2.ppt").show()
//    spark.sql("show columns in cat.cls_db2.v1").show()





  }

  def normalPathApp(spark: SparkSession):Unit ={
    spark.read.format("csv").option("inferSchema","true").option("header", "true").load("/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data").show()
  }

  def dataframeWriteOperation(spark: SparkSession):Unit ={
    spark.sql("""create database if not exists cat.cls_db3""")
    val df =  spark.read.format("csv").option("inferSchema","true").option("header", "true").load("/Users/sharadsingh/Dev/databricks-copy-into/src/main/resources/data")
    df.write.format("parquet").saveAsTable("cat.cls_db3.ptbl")
    df.write.format("delta").saveAsTable("cat.cls_db3.dtbl")
    df.write.format("iceberg").saveAsTable("cat.cls_db3.itbl")

  }


  def icebergApp(spark:SparkSession):Unit={
    spark.sql("""create database if not exists cat.cls_db4""")

    spark.sql("create table cat.cls_db4.it(id int, name string) using iceberg PARTITIONED BY (name)")
    spark.sql("insert into cat.cls_db4.it values(1,'sh'), (2, 'su')")

    spark.sql(
      """
        |UPDATE cat.cls_db4.it
        |SET id = 3
        |WHERE name = 'sh';
        |""".stripMargin)
    println("**** first read after update ****")
    spark.read.table("cat.cls_db4.it").show()

    spark.sql(
      """
        |DELETE FROM cat.cls_db4.it
        |WHERE id = 3;
        |
        |""".stripMargin)


    println("**** second read after delete ****")
    spark.read.table("cat.cls_db4.it").show()



  }


  def deltaforNameApp(spark: SparkSession):Unit = {

    import spark.implicits._
    spark.sql("create database db")
    spark.sql("create table db.dt(id int, name string) using delta")
    spark.sql("insert into db.dt values (1, 'ss'), (2,'sh')")
//     var df3 = Seq(
//  (7, "John", 2.0),
//  (8, "Sunny", 3.0),
//  (9, "Xiaoyu", 4.0),
//  (10, "Shashi", 5.0),
//  (11, "Bharath", 6.0),
//  (12, "Vivek", 7.0)
//   ).toDF("col1", "col2", "col3")
//    spark.sql("create database default")

   // df3.write.partitionBy("col2").format("delta").mode(SaveMode.Overwrite).saveAsTable("dt")

    val dt = io.delta.tables.hc.DeltaTable.getDeltaTable(spark, "db.dt", "/tmp/dt")

   // dt.delete("id = 1")

  //  dt.toDF.show()

  //  spark.sql("select * from db.dt").show()

    val sourceDf = createMergeSourceDF(spark)
    dt.as("target").merge(sourceDf.as("source"),
      "target.id = source.id").whenMatched()
      .updateExpr(Map(
        "name" -> "source.name"
      )).execute()

    spark.sql("select * from db.dt").show()

  }

  def createMergeSourceDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)

    ))

    val data = Seq(
      Row(2, "Bob Updated"), // matched row -> update
      Row(4, "David") // new row -> insert
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }


  def syncSchema(spark:SparkSession): Unit = {
    spark.sql("create database cat.ddb11")
    spark.sql("create table cat.ddb11.t1(id int, cls_a int, cls_b int ) using delta location '/tmp/ddt'")
    spark.sql("insert into cat.ddb11.t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)")

    spark.sql("create table cat.ddb11.t2 using delta location '/tmp/ddt'")
    spark.sql("describe formatted cat.ddb11.t2").show()

  }


  def timeTravel(spark: SparkSession): Unit ={
    spark.sql("create database cat.ddb12")
    spark.sql("create table cat.ddb12.t1(id int, cls_a int, cls_b int ) using delta")
    spark.sql("insert into cat.ddb12.t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
    spark.sql("insert into cat.ddb12.t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
    spark.sql("insert into cat.ddb12.t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)")

    spark.sql("SELECT * FROM cat.ddb12.t1 VERSION AS OF 1").show()
    val df = spark.sql("SELECT * FROM cat.ddb12.t1")
    df.show()
    df.toJSON.show(10,false)
  }



  def view_avro(spark:SparkSession):Unit={
    spark.sql("CREATE SCHEMA IF NOT EXISTS cat.teste")

    val raw_table = "cat.teste.pr439_avro_view_raw"
    val view_name = "cat.teste.pr439_avro_view_star"

   // spark.sql(f"DROP VIEW IF EXISTS {view_name}")
 //   spark.sql(f"DROP TABLE IF EXISTS {raw_table}")

    spark.sql(
      s"""
    CREATE TABLE ${raw_table} (
      op_type STRING,
      `table` STRING,
      op_ts STRING,
      current_ts STRING,
      pos STRING,
      IDT_TRANSACTION_TRANCOST DOUBLE,
      IDT_TRANSACTION BIGINT,
      NUM_TRANSACTION_COST_VALUE DOUBLE,
      IDT_TRANSACTION_COST BIGINT,
      IDT_USER_APPLICATION BIGINT,
      COD_COST STRING,
      IND_EXTERNAL_SYSTEM BIGINT,
      DAT_PURGE_REFERENCE STRING,
      dat_kafka TIMESTAMP,
      day STRING
    ) USING AVRO
    """)

    spark.sql(
      s"""
    INSERT INTO ${raw_table} VALUES (
      'I',
      'SAFEPAY_ADM.TRANS',
      '2026-06-02T04:47:00Z',
      '2026-06-02T04:47:00Z',
      '1',
      1.0,
      10L,
      2.0,
      20L,
      30L,
      'COST',
      0L,
      'PURGE',
      TIMESTAMP '2026-06-02 04:47:00',
      '2026-06-02_04_45'
    )
    """)

    print("1. Direct Avro query should succeed")
    spark.sql(
      s"""
    SELECT op_type, `table`, current_ts, dat_kafka, day
    FROM ${raw_table}
    WHERE day >= '2026-06-02_04_44'
      AND day <= '2026-06-07_14_45'
    LIMIT 1
    """).show()

    print("2. Create saved view")
    spark.sql(
      s"""
    CREATE VIEW ${view_name} AS
    SELECT curated.*
    FROM (
      SELECT
        CAST(s.op_type AS STRING) AS op_type,
        CAST(s.`table` AS STRING) AS `table`,
        CAST(s.op_ts AS STRING) AS op_ts,
        CAST(s.current_ts AS STRING) AS current_ts,
        CAST(s.pos AS STRING) AS pos,
        CAST(s.IDT_TRANSACTION_TRANCOST AS DOUBLE) AS IDT_TRANSACTION_TRANCOST,
        CAST(s.IDT_TRANSACTION AS BIGINT) AS IDT_TRANSACTION,
        CAST(s.NUM_TRANSACTION_COST_VALUE AS DOUBLE) AS NUM_TRANSACTION_COST_VALUE,
        CAST(s.IDT_TRANSACTION_COST AS BIGINT) AS IDT_TRANSACTION_COST,
        CAST(s.IDT_USER_APPLICATION AS BIGINT) AS IDT_USER_APPLICATION,
        CAST(s.COD_COST AS STRING) AS COD_COST,
        CAST(s.IND_EXTERNAL_SYSTEM AS BIGINT) AS IND_EXTERNAL_SYSTEM,
        CAST(s.DAT_PURGE_REFERENCE AS STRING) AS DAT_PURGE_REFERENCE,
        to_timestamp(s.dat_kafka) AS dat_kafka,
        CAST(current_timestamp AS TIMESTAMP) AS dat_import_utc,
        ROW_NUMBER() OVER (
          PARTITION BY idt_transaction_trancost
          ORDER BY current_ts DESC
        ) AS num
      FROM ${raw_table} s
      WHERE day >= '2026-06-02_04_44'
        AND day <= '2026-06-07_14_45'
    ) curated
    WHERE num = 1
    """)

    println("3. Read saved view. This is expected to reproduce the issue.")
    val df = spark.sql(s"SELECT * FROM ${view_name}")
    df.show()
    df.printSchema()
    df.toJSON.show()

  }

  def withCTE(spark:SparkSession):Unit={

    spark.sql("CREATE SCHEMA IF NOT EXISTS cat.teste1")

    val raw_table = "cat.teste1.pr439_avro_view_raw"
    val view_name = "cat.teste1.pr439_avro_view_star"

   // val fullyQualifiedView = s"cat1.default.$viewName"


    spark.sql(
      s"""
         |CREATE TABLE $raw_table (
         |  op_type STRING,
         |  `table` STRING,
         |  current_ts STRING,
         |  IDT_TRANSACTION_TRANCOST DOUBLE,
         |  quantity INT,
         |  state STRING,
         |  dat_kafka TIMESTAMP,
         |  day STRING
         |) USING PARQUET
         |TBLPROPERTIES (
         |  'test.secure.columns' =
         |  'op_type,table,current_ts,IDT_TRANSACTION_TRANCOST,quantity,state,dat_kafka,day'
         |)
         |""".stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $raw_table VALUES
         |  ('I', 'SAFEPAY_ADM.TRANS', '2026-06-02T04:47:00Z',
         |   1.25, 2, 'CA', TIMESTAMP '2026-06-02 04:47:00', '2026-06-02_04_45'),
         |  ('U', 'SAFEPAY_ADM.TRANS', '2026-06-02T04:50:00Z',
         |   3.75, 1, 'CA', TIMESTAMP '2026-06-02 04:50:00', '2026-06-02_04_50'),
         |  ('I', 'SAFEPAY_ADM.SETTLE', '2026-06-03T05:01:00Z',
         |   4.0, 3, 'WA', TIMESTAMP '2026-06-03 05:01:00', '2026-06-03_05_00'),
         |  ('D', 'SAFEPAY_ADM.TRANS', '2026-06-01T00:00:00Z',
         |   10.0, 1, 'CA', TIMESTAMP '2026-06-01 00:00:00', '2026-06-01_00_00')
         |""".stripMargin)
    val viewSQl = s"""
       |CREATE VIEW $view_name AS
       |WITH normalized AS (
       |  SELECT
       |    `table`,
       |    CASE
       |      WHEN IDT_TRANSACTION_TRANCOST >= 4.0 THEN 'high'
       |      ELSE 'normal'
       |    END AS cost_bucket,
       |    quantity,
       |    op_type
       |  FROM $raw_table
       |  WHERE day >= '2026-06-02_00_00'
       |),
       |filtered AS (
       |  SELECT *
       |  FROM normalized
       |  WHERE op_type IN ('I', 'U')
       |)
       |SELECT
       |  cost_bucket,
       |  `table`,
       |  COUNT(*) AS event_count,
       |  SUM(quantity) AS total_quantity
       |FROM filtered
       |GROUP BY cost_bucket, `table`
       |""".stripMargin
    spark.sql(viewSQl)

    spark.sql(s"describe formatted $view_name").show()

    spark.sql(s"select * from ${view_name}").show()


  }


  def view_usage_test1(spark: SparkSession): Unit = {
    spark.sql("CREATE SCHEMA IF NOT EXISTS cat.cls_view_db")
    val table1 = "dtbl"
    val table2 = "ptbl"
    val tableName1 = "cat.cls_view_db.dtbl"
    val tableName2 = "cat.cls_view_db.ptbl"

    val viewName1 = "cat.cls_view_db.v_dtbl"
    val viewName2 = "cat.cls_view_db.v_ptbl"
    spark.sql(
      s"""
         |CREATE TABLE $tableName1 (
         | order_id STRING,
         | cls_customer_id STRING,
         | amount DOUBLE,
         | cls_order_date DATE
         |) USING delta
         |PARTITIONED BY (cls_order_date)
         |""".stripMargin)

    spark.sql(
      s"""
         |CREATE TABLE $tableName2 (
         | order_id STRING,
         | cls_customer_id STRING,
         | amount DOUBLE,
         | cls_order_date DATE
         |) USING parquet
         |PARTITIONED BY (cls_order_date)
         |""".stripMargin)


    spark.sql(
      s"""
         |INSERT INTO $tableName1 VALUES
         | ('ORD-001', 'CUST-101', 125.5, DATE '2024-01-10'),
         | ('ORD-002', 'CUST-102', 75.0, DATE '2024-02-20'),
         | ('ORD-003', 'CUST-101', 250.0, DATE '2023-12-15'),
         | ('ORD-004', 'CUST-103', 20.0, DATE '2022-11-03')
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $tableName2 VALUES
         | ('ORD-001', 'CUST-101', 125.5, DATE '2024-01-10'),
         | ('ORD-002', 'CUST-102', 75.0, DATE '2024-02-20'),
         | ('ORD-003', 'CUST-101', 250.0, DATE '2023-12-15'),
         | ('ORD-004', 'CUST-103', 20.0, DATE '2022-11-03')
         |""".stripMargin)

    println("----- Insertion completed -----")

    spark.sql(
      s"""
         |CREATE VIEW $viewName1
         |AS SELECT order_id, cls_customer_id, amount, cls_order_date
         |FROM $tableName1
         |""".stripMargin)

    spark.sql(
      s"""
         |CREATE VIEW $viewName2
         |AS SELECT order_id, cls_customer_id, amount, cls_order_date
         |FROM $tableName2
         |""".stripMargin)

    println("----- Creation of Views completed -----")

//   // val p = (new SparkSqlParser()).parsePlan(
//      s"""
//         |SELECT a.cls_customer_id, COUNT(*) FROM ${viewName1} a JOIN ${viewName2} b
//         |ON a.cls_customer_id = b.cls_customer_id GROUP BY a.cls_customer_id
//         |HAVING COUNT (*) > ( SELECT COUNT (*) FROM  ${viewName2} WHERE order_id IS NOT NULL)
//         |""".stripMargin)

  //  println("----- Parsing completed -----")

//    val df = spark.sql(
//      s"""
//         |SELECT a.cls_customer_id, COUNT(*) FROM ${viewName1} a JOIN ${viewName2} b
//         |ON a.cls_customer_id = b.cls_customer_id GROUP BY a.cls_customer_id
//         |HAVING COUNT (*) > ( SELECT COUNT (*) FROM  ${viewName2} WHERE order_id IS NOT NULL)
//         |""".stripMargin
//    )

    val df = spark.sql(
      s"""
         |SELECT a.cls_customer_id, COUNT(*) FROM ${viewName1} a JOIN ${viewName2} b
         |ON a.cls_customer_id = b.cls_customer_id GROUP BY a.cls_customer_id
         |HAVING COUNT (*) > ( SELECT COUNT (*) FROM  ${viewName2} )
         |""".stripMargin
    )

    df.explain(true)
    df.show()


  }

}
