/**
 * Overwrite format combination tests — covers df.write.mode("overwrite").saveAsTable
 * and df.write.format(X).mode("overwrite").saveAsTable for all format transitions.
 *
 * Formats tested: parquet, orc, csv, json, avro, delta, iceberg (hudi excluded)
 *
 * Each test case creates its own db and table — no shared state between cases.
 * SparkSession supplied by caller (App.main) — same pattern as IcebergProcedureTests.
 * Catalog prefix: "cat"
 */
object OverwriteFormatTests {

  // ─── helpers ──────────────────────────────────────────────────────────────

  private def sep(name: String): Unit =
    println(s"\n${"=" * 70}\n  $name\n${"=" * 70}")

  private def freshTable(spark: org.apache.spark.sql.SparkSession,
                         db: String, tbl: String, format: String): String = {
    val fqn = s"cat.$db.$tbl"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS cat.$db")
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    val extra = if (format == "iceberg") "TBLPROPERTIES ('format-version' = '2')" else ""
    spark.sql(
      s"""CREATE TABLE $fqn (id INT, name STRING, val DOUBLE)
         |USING $format $extra""".stripMargin)
    spark.sql(s"INSERT INTO $fqn VALUES (1,'Alice',1.0),(2,'Bob',2.0)")
    spark.sql(s"INSERT INTO $fqn VALUES (3,'Carol',3.0)")
    fqn
  }

  private def count(spark: org.apache.spark.sql.SparkSession, fqn: String): Long =
    spark.sql(s"SELECT COUNT(*) FROM $fqn").collect()(0).getLong(0)

  private def provider(spark: org.apache.spark.sql.SparkSession, fqn: String): String =
    spark.sql(s"DESCRIBE EXTENDED $fqn")
      .filter("col_name = 'Provider'")
      .collect()(0).getString(1)

  private def overwriteData(spark: org.apache.spark.sql.SparkSession)
  : org.apache.spark.sql.DataFrame =
    spark.createDataFrame(Seq((10, "Overwrite1", 10.0), (11, "Overwrite2", 11.0)))
      .toDF("id", "name", "val")

  private def verify(spark: org.apache.spark.sql.SparkSession,
                     fqn: String, expectedCount: Long, expectedProvider: String): Unit = {
    val c = count(spark, fqn)
    val p = provider(spark, fqn)
    println(s"  count=$c (expected $expectedCount)  provider=$p (expected $expectedProvider)")
    assert(c == expectedCount,
      s"count mismatch on $fqn: got $c expected $expectedCount")
    assert(p.equalsIgnoreCase(expectedProvider),
      s"provider mismatch on $fqn: got $p expected $expectedProvider")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 1. same format overwrites
  // ══════════════════════════════════════════════════════════════════════════

  def testParquetToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → parquet")
    val fqn = freshTable(spark, "ow_parq_parq", "t1", "parquet")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testOrcToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → orc")
    val fqn = freshTable(spark, "ow_orc_orc", "t1", "orc")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testCsvToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → csv")
    val fqn = freshTable(spark, "ow_csv_csv", "t1", "csv")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testJsonToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → json")
    val fqn = freshTable(spark, "ow_json_json", "t1", "json")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testAvroToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → avro")
    val fqn = freshTable(spark, "ow_avro_avro", "t1", "avro")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testDeltaToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → delta (transactional overwrite — history preserved)")
    val fqn = freshTable(spark, "ow_delta_delta", "t1", "delta")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testIcebergToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → iceberg (transactional overwrite — history preserved)")
    val fqn = freshTable(spark, "ow_ice_ice", "t1", "iceberg")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 2. no explicit format — transactional preserved, file format uses default
  // ══════════════════════════════════════════════════════════════════════════

  def testNoFormatExistingDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=delta → delta preserved")
    val fqn = freshTable(spark, "ow_nofmt_delta", "t1", "delta")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testNoFormatExistingIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=iceberg → iceberg preserved")
    val fqn = freshTable(spark, "ow_nofmt_ice", "t1", "iceberg")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  def testNoFormatExistingParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=parquet → default format applied")
    val fqn = freshTable(spark, "ow_nofmt_parq", "t1", "parquet")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  def testNoFormatExistingOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=orc → default format applied")
    val fqn = freshTable(spark, "ow_nofmt_orc", "t1", "orc")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  def testNoFormatExistingCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=csv → default format applied")
    val fqn = freshTable(spark, "ow_nofmt_csv", "t1", "csv")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  def testNoFormatExistingJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=json → default format applied")
    val fqn = freshTable(spark, "ow_nofmt_json", "t1", "json")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  def testNoFormatExistingAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("no explicit format + existing=avro → default format applied")
    val fqn = freshTable(spark, "ow_nofmt_avro", "t1", "avro")
    println(s"before: count=${count(spark, fqn)}")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 3. parquet → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testParquetToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → orc")
    val fqn = freshTable(spark, "ow_parq_orc", "t1", "parquet")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testParquetToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → csv")
    val fqn = freshTable(spark, "ow_parq_csv", "t1", "parquet")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testParquetToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → json")
    val fqn = freshTable(spark, "ow_parq_json", "t1", "parquet")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testParquetToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → avro")
    val fqn = freshTable(spark, "ow_parq_avro", "t1", "parquet")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testParquetToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → delta")
    val fqn = freshTable(spark, "ow_parq_delta", "t1", "parquet")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testParquetToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("parquet → iceberg")
    val fqn = freshTable(spark, "ow_parq_ice", "t1", "parquet")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 4. delta → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testDeltaToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → parquet")
    val fqn = freshTable(spark, "ow_delta_parq", "t1", "delta")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testDeltaToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → orc")
    val fqn = freshTable(spark, "ow_delta_orc", "t1", "delta")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testDeltaToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → csv")
    val fqn = freshTable(spark, "ow_delta_csv", "t1", "delta")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testDeltaToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → json")
    val fqn = freshTable(spark, "ow_delta_json", "t1", "delta")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testDeltaToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → avro")
    val fqn = freshTable(spark, "ow_delta_avro", "t1", "delta")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testDeltaToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("delta → iceberg")
    val fqn = freshTable(spark, "ow_delta_ice", "t1", "delta")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 5. iceberg → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testIcebergToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → parquet")
    val fqn = freshTable(spark, "ow_ice_parq", "t1", "iceberg")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testIcebergToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → orc")
    val fqn = freshTable(spark, "ow_ice_orc", "t1", "iceberg")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testIcebergToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → csv")
    val fqn = freshTable(spark, "ow_ice_csv", "t1", "iceberg")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testIcebergToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → json")
    val fqn = freshTable(spark, "ow_ice_json", "t1", "iceberg")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testIcebergToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → avro")
    val fqn = freshTable(spark, "ow_ice_avro", "t1", "iceberg")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testIcebergToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("iceberg → delta")
    val fqn = freshTable(spark, "ow_ice_delta", "t1", "iceberg")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 6. orc → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testOrcToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → parquet")
    val fqn = freshTable(spark, "ow_orc_parq", "t1", "orc")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testOrcToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → csv")
    val fqn = freshTable(spark, "ow_orc_csv", "t1", "orc")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testOrcToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → json")
    val fqn = freshTable(spark, "ow_orc_json", "t1", "orc")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testOrcToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → avro")
    val fqn = freshTable(spark, "ow_orc_avro", "t1", "orc")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testOrcToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → delta")
    val fqn = freshTable(spark, "ow_orc_delta", "t1", "orc")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testOrcToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("orc → iceberg")
    val fqn = freshTable(spark, "ow_orc_ice", "t1", "orc")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 7. csv → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testCsvToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → parquet")
    val fqn = freshTable(spark, "ow_csv_parq", "t1", "csv")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testCsvToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → orc")
    val fqn = freshTable(spark, "ow_csv_orc", "t1", "csv")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testCsvToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → json")
    val fqn = freshTable(spark, "ow_csv_json", "t1", "csv")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testCsvToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → avro")
    val fqn = freshTable(spark, "ow_csv_avro", "t1", "csv")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testCsvToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → delta")
    val fqn = freshTable(spark, "ow_csv_delta", "t1", "csv")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testCsvToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("csv → iceberg")
    val fqn = freshTable(spark, "ow_csv_ice", "t1", "csv")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 8. json → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testJsonToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → parquet")
    val fqn = freshTable(spark, "ow_json_parq", "t1", "json")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testJsonToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → orc")
    val fqn = freshTable(spark, "ow_json_orc", "t1", "json")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testJsonToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → csv")
    val fqn = freshTable(spark, "ow_json_csv", "t1", "json")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testJsonToAvro(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → avro")
    val fqn = freshTable(spark, "ow_json_avro", "t1", "json")
    overwriteData(spark).write.format("avro").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "avro")
  }

  def testJsonToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → delta")
    val fqn = freshTable(spark, "ow_json_delta", "t1", "json")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testJsonToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("json → iceberg")
    val fqn = freshTable(spark, "ow_json_ice", "t1", "json")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 9. avro → other formats
  // ══════════════════════════════════════════════════════════════════════════

  def testAvroToParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → parquet")
    val fqn = freshTable(spark, "ow_avro_parq", "t1", "avro")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testAvroToOrc(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → orc")
    val fqn = freshTable(spark, "ow_avro_orc", "t1", "avro")
    overwriteData(spark).write.format("orc").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "orc")
  }

  def testAvroToCsv(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → csv")
    val fqn = freshTable(spark, "ow_avro_csv", "t1", "avro")
    overwriteData(spark).write.format("csv").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "csv")
  }

  def testAvroToJson(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → json")
    val fqn = freshTable(spark, "ow_avro_json", "t1", "avro")
    overwriteData(spark).write.format("json").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "json")
  }

  def testAvroToDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → delta")
    val fqn = freshTable(spark, "ow_avro_delta", "t1", "avro")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testAvroToIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("avro → iceberg")
    val fqn = freshTable(spark, "ow_avro_ice", "t1", "avro")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 10. new table (orCreate) — table does not exist
  // ══════════════════════════════════════════════════════════════════════════

  def testNewTableParquet(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("new table + explicit parquet")
    val fqn = s"cat.ow_new_parq.t1"
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ow_new_parq")
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    overwriteData(spark).write.format("parquet").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "parquet")
  }

  def testNewTableDelta(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("new table + explicit delta")
    val fqn = s"cat.ow_new_delta.t1"
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ow_new_delta")
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    overwriteData(spark).write.format("delta").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "delta")
  }

  def testNewTableIceberg(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("new table + explicit iceberg")
    val fqn = s"cat.ow_new_ice.t1"
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ow_new_ice")
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    overwriteData(spark).write.format("iceberg").mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2, expectedProvider = "iceberg")
  }

  def testNewTableNoFormat(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("new table + no explicit format → default format")
    val fqn = s"cat.ow_new_nofmt.t1"
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ow_new_nofmt")
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    overwriteData(spark).write.mode("overwrite").saveAsTable(fqn)
    verify(spark, fqn, expectedCount = 2,
      expectedProvider = spark.sessionState.conf.defaultDataSourceName)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // run all
  // ══════════════════════════════════════════════════════════════════════════

  def runAll(spark: org.apache.spark.sql.SparkSession): Unit = {
    // same format
    testParquetToParquet(spark)
    testOrcToOrc(spark)
    testCsvToCsv(spark)
    testJsonToJson(spark)
    testAvroToAvro(spark)
    testDeltaToDelta(spark)
    testIcebergToIceberg(spark)
    // no explicit format
    testNoFormatExistingDelta(spark)
    testNoFormatExistingIceberg(spark)
    testNoFormatExistingParquet(spark)
    testNoFormatExistingOrc(spark)
    testNoFormatExistingCsv(spark)
    testNoFormatExistingJson(spark)
    testNoFormatExistingAvro(spark)
    // parquet →
    testParquetToOrc(spark)
    testParquetToCsv(spark)
    testParquetToJson(spark)
    testParquetToAvro(spark)
    testParquetToDelta(spark)
    testParquetToIceberg(spark)
    // delta →
    testDeltaToParquet(spark)
    testDeltaToOrc(spark)
    testDeltaToCsv(spark)
    testDeltaToJson(spark)
    testDeltaToAvro(spark)
    testDeltaToIceberg(spark)
    // iceberg →
    testIcebergToParquet(spark)
    testIcebergToOrc(spark)
    testIcebergToCsv(spark)
    testIcebergToJson(spark)
    testIcebergToAvro(spark)
    testIcebergToDelta(spark)
    // orc →
    testOrcToParquet(spark)
    testOrcToCsv(spark)
    testOrcToJson(spark)
    testOrcToAvro(spark)
    testOrcToDelta(spark)
    testOrcToIceberg(spark)
    // csv →
    testCsvToParquet(spark)
    testCsvToOrc(spark)
    testCsvToJson(spark)
    testCsvToAvro(spark)
    testCsvToDelta(spark)
    testCsvToIceberg(spark)
    // json →
    testJsonToParquet(spark)
    testJsonToOrc(spark)
    testJsonToCsv(spark)
    testJsonToAvro(spark)
    testJsonToDelta(spark)
    testJsonToIceberg(spark)
    // avro →
    testAvroToParquet(spark)
    testAvroToOrc(spark)
    testAvroToCsv(spark)
    testAvroToJson(spark)
    testAvroToDelta(spark)
    testAvroToIceberg(spark)
    // new table
    testNewTableParquet(spark)
    testNewTableDelta(spark)
    testNewTableIceberg(spark)
    testNewTableNoFormat(spark)
    println("\n✓ all overwrite format combination tests done")
  }
}