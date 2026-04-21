import org.apache.spark.sql.{SaveMode, SparkSession}

object ErrorApp {

  def setErrorConf(spark:SparkSession):Unit={
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    // Small advisory size forces aggressive coalescing of 200 → few partitions
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1b")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
    // Start with 200 shuffle partitions so AQE collapses them heavily
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    // plannedWrite must be ON (default in Spark 3.4+) — adds WriteFilesExec
    spark.conf.set("spark.sql.plannedWrite.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
  }

  def reproduce(spark: SparkSession): Unit = {


    setErrorConf(spark)
    // ── Step 1: create two small DataFrames ─────────────────────────────────
    // Small data → AQE will coalesce 200 shuffle partitions → very few
    val left = spark.range(0, 500).toDF("id")
      .withColumn("value", org.apache.spark.sql.functions.lit("left_val"))

    val right = spark.range(0, 500).toDF("id")
      .withColumn("label", org.apache.spark.sql.functions.lit("right_label"))

    // ── Step 2: a query that forces a shuffle (join → SortMergeJoin) ─────────
    // The join creates a ShuffleExchange. AQE coalesces post-shuffle partitions
    // to e.g. 1-5. Without EnsureRequirements both sides must already be
    // co-partitioned — qe.sparkPlan does NOT guarantee this.
    val joinedQuery = left.join(right, Seq("id"), "inner")
      .groupBy("id")
      .agg(
        org.apache.spark.sql.functions.first("value").alias("value"),
        org.apache.spark.sql.functions.first("label").alias("label")
      )


    joinedQuery.write.partitionBy("label").format("parquet").mode(SaveMode.Append).saveAsTable("ine007")
    spark.sql("select count(*) from ine007").show()
    joinedQuery.write.partitionBy("label").format("parquet").mode(SaveMode.Append).saveAsTable("ine007")
    spark.sql("select count(*) from ine007").show()
    spark.read.table("ine007").show()
  }



  def reproduce_zip(spark: SparkSession): Unit = {
    setErrorConf(spark)
    import spark.implicits._

    val LEFT_PARTITIONS = 47244 // matches List(24722, **47244**)
    val RIGHT_PARTITIONS = 24722 // matches List(**24722**, 47244)

    val left = spark.range(0, 10000)
      .toDF("id")
      .withColumn("category",
        (org.apache.spark.sql.functions.col("id") % 5)
          .cast("string"))
      .withColumn("left_value",
        org.apache.spark.sql.functions.col("id") * 2.0).repartition(LEFT_PARTITIONS)

    val right = spark.range(0, 10000)
      .toDF("id")
      .withColumn("category",
        (org.apache.spark.sql.functions.col("id") % 5)
          .cast("string"))
      .withColumn("right_value",
        org.apache.spark.sql.functions.col("id") * 3.0).repartition(RIGHT_PARTITIONS)


    var joinedQuery = left.join(right, Seq("id"), "inner")
      .select(
        left("id"),
        left("category"),
        left("left_value"),
        right("right_value"))

    joinedQuery = joinedQuery.select("id", "left_value", "right_value", "category")

    joinedQuery.write.partitionBy("category").format("parquet").mode(SaveMode.Append).saveAsTable("ine0004")
    spark.sql("select count(*) from ine0004").show()
    joinedQuery.write.partitionBy("category").format("parquet").mode(SaveMode.Append).saveAsTable("ine0004")
    spark.sql("select count(*) from ine0004").show()
    // spark.read.table("ine0004").show()
    joinedQuery.write.mode(SaveMode.Append).saveAsTable("ine0004")
    spark.sql("select count(*) from ine0004").show()
    spark.read.table("ine0004").show()


  }

  def reproduce_zip_short(spark: SparkSession): Unit = {
    setErrorConf(spark)
    import spark.implicits._

    val LEFT_PARTITIONS = 5 // matches List(24722, **47244**)
    val RIGHT_PARTITIONS = 10 // matches List(**24722**, 47244)

    val left = spark.range(0, 100)
      .toDF("id")
      .withColumn("category",
        (org.apache.spark.sql.functions.col("id") % 5)
          .cast("string"))
      .withColumn("left_value",
        org.apache.spark.sql.functions.col("id") * 2.0).repartition(LEFT_PARTITIONS)

    val right = spark.range(0, 100)
      .toDF("id")
      .withColumn("category",
        (org.apache.spark.sql.functions.col("id") % 5)
          .cast("string"))
      .withColumn("right_value",
        org.apache.spark.sql.functions.col("id") * 3.0).repartition(RIGHT_PARTITIONS)


    var joinedQuery = left.join(right, Seq("id"), "inner")
      .select(
        left("id"),
        left("category"),
        left("left_value"),
        right("right_value"))

    joinedQuery = joinedQuery.select("id", "left_value", "right_value", "category")

    joinedQuery.write.partitionBy("category").format("parquet").mode(SaveMode.Append).saveAsTable("ine0006")
  //  spark.sql("select count(*) from ine0006").show()
    joinedQuery.write.partitionBy("category").format("parquet").mode(SaveMode.Append).saveAsTable("ine0006")
  //  spark.sql("select count(*) from ine0005").show()
    // spark.read.table("ine0004").show()
    joinedQuery.write.mode(SaveMode.Append).saveAsTable("ine0006")
    spark.sql("select count(*) from ine0006").show()
    spark.read.table("ine0006").show()


  }
}
