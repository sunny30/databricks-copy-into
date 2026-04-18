import org.apache.spark.sql.{SaveMode, SparkSession}

object ErrorApp {

  def reproduce(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Small advisory size forces aggressive coalescing of 200 → few partitions
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1b")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
      // Start with 200 shuffle partitions so AQE collapses them heavily
    spark.conf.set("spark.sql.shuffle.partitions", "200")
      // plannedWrite must be ON (default in Spark 3.4+) — adds WriteFilesExec
    spark.conf.set("spark.sql.plannedWrite.enabled", "true")

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


    joinedQuery.write.partitionBy("label").format("parquet").mode(SaveMode.Append).saveAsTable("ine005")
    spark.sql("select count(*) from ine005").show()
    joinedQuery.write.partitionBy("label").format("parquet").mode(SaveMode.Append).saveAsTable("ine005")
    spark.sql("select count(*) from ine005").show()
    spark.read.table("ine005").show()
  }

  }
