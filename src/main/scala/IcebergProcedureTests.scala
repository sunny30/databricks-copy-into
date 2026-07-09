/**
 * Manual test runner for every Iceberg stored-procedure variation.
 *
 * SparkSession is supplied by the caller (App.main), built via App.getConf +
 * enableHiveSupport(), so UnityCatalog / CustomExtensionSuite / FSMetaStoreCatalog
 * are already in place — no extra bootstrap here.
 *
 * Catalog prefix: "cat"  (spark_catalog = UnityCatalog, alias "cat" used throughout
 * the project — matches CLSApp.scala / App.scala convention).
 *
 * Usage from App.main:
 *   IcebergProcedureTests.testRollbackToSnapshot(spark)
 *   // ... or run them all:
 *   IcebergProcedureTests.runAll(spark)
 */
object IcebergProcedureTests {

  // ─── shared helpers ────────────────────────────────────────────────────────

  private def sep(name: String): Unit =
    println(s"\n${"=" * 70}\n  $name\n${"=" * 70}")

  private def setup(spark: org.apache.spark.sql.SparkSession, db: String, tbl: String,
                    partitioned: Boolean = true): String = {
    val fqn = s"cat.$db.$tbl"
    spark.sql(s"DROP TABLE IF EXISTS $fqn")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS cat.$db")
    val part = if (partitioned) "PARTITIONED BY (name)" else ""
    spark.sql(
      s"""CREATE TABLE $fqn (id INT, name STRING, val DOUBLE)
         |USING iceberg $part
         |TBLPROPERTIES ('format-version' = '2')""".stripMargin)
    spark.sql(s"INSERT INTO $fqn VALUES (1,'Alice',1.0),(2,'Bob',2.0)")
    spark.sql(s"INSERT INTO $fqn VALUES (3,'Carol',3.0)")
    spark.sql(s"INSERT INTO $fqn VALUES (4,'Dave',4.0)")
    fqn
  }

  private def snapshotIds(spark: org.apache.spark.sql.SparkSession, fqn: String): Seq[Long] =
    spark.sql(s"SELECT snapshot_id FROM $fqn.snapshots ORDER BY committed_at")
      .collect().map(_.getLong(0)).toSeq

  private def count(spark: org.apache.spark.sql.SparkSession, fqn: String): Long =
    spark.sql(s"SELECT COUNT(*) FROM $fqn").collect()(0).getLong(0)

  // ══════════════════════════════════════════════════════════════════════════
  // 1. rollback_to_snapshot
  // ══════════════════════════════════════════════════════════════════════════

  def testRollbackToSnapshot(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("rollback_to_snapshot")
    val fqn = setup(spark, "ice_db1", "t1")
    val ids  = snapshotIds(spark, fqn)
    println(s"snapshots: $ids  |  row count: ${count(spark, fqn)}")

    // positional args
    spark.sql(s"CALL cat.system.rollback_to_snapshot('$fqn', ${ids.head})")
    println(s"[positional] count after rollback to snapshot[0]: ${count(spark, fqn)}")

    spark.sql(s"INSERT INTO $fqn VALUES (5,'Eve',5.0)")

    // named args
    spark.sql(s"CALL cat.system.rollback_to_snapshot(table => '$fqn', snapshot_id => ${ids.head})")
    println(s"[named]      count after rollback to snapshot[0]: ${count(spark, fqn)}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 2. rollback_to_timestamp
  // ══════════════════════════════════════════════════════════════════════════

  def testRollbackToTimestamp(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("rollback_to_timestamp")
    val fqn = setup(spark, "ice_db2", "t1")
    val firstTs = spark
      .sql(s"SELECT committed_at FROM $fqn.snapshots ORDER BY committed_at")
      .collect()(0).getTimestamp(0).toString
    println(s"first commit at: $firstTs  |  row count: ${count(spark, fqn)}")

    // positional args
    spark.sql(s"CALL cat.system.rollback_to_timestamp('$fqn', TIMESTAMP '$firstTs')")
    println(s"[positional] count after rollback: ${count(spark, fqn)}")

    spark.sql(s"INSERT INTO $fqn VALUES (5,'Eve',5.0)")

    // named args
    spark.sql(
      s"""CALL cat.system.rollback_to_timestamp(
         |  table     => '$fqn',
         |  timestamp => TIMESTAMP '$firstTs'
         |)""".stripMargin)
    println(s"[named]      count after rollback: ${count(spark, fqn)}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 3. set_current_snapshot
  // ══════════════════════════════════════════════════════════════════════════

  def testSetCurrentSnapshot(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("set_current_snapshot")
    val fqn = setup(spark, "ice_db3", "t1")
    val ids  = snapshotIds(spark, fqn)

    // positional args
    spark.sql(s"CALL cat.system.set_current_snapshot('$fqn', ${ids.head})")
    println(s"[positional] count after set to snapshot[0]: ${count(spark, fqn)}")

    // named args
    spark.sql(s"CALL cat.system.set_current_snapshot(table => '$fqn', snapshot_id => ${ids.last})")
    println(s"[named]      count after set to snapshot[-1]: ${count(spark, fqn)}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 4. cherrypick_snapshot
  // ══════════════════════════════════════════════════════════════════════════

  def testCherrypickSnapshot(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("cherrypick_snapshot")
    val fqn = setup(spark, "ice_db4", "t1")
    val ids  = snapshotIds(spark, fqn)
    spark.sql(s"CALL cat.system.rollback_to_snapshot('$fqn', ${ids.head})")
    println(s"after rollback to snapshot[0]: ${count(spark, fqn)}")

    // positional args
    spark.sql(s"CALL cat.system.cherrypick_snapshot('$fqn', ${ids.last})")
    println(s"[positional] count after cherrypick of snapshot[-1]: ${count(spark, fqn)}")

    spark.sql(s"CALL cat.system.rollback_to_snapshot('$fqn', ${ids.head})")

    // named args
    spark.sql(s"CALL cat.system.cherrypick_snapshot(table => '$fqn', snapshot_id => ${ids.last})")
    println(s"[named]      count after cherrypick of snapshot[-1]: ${count(spark, fqn)}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 5. fast_forward
  // ══════════════════════════════════════════════════════════════════════════

  def testFastForward(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("fast_forward")
    val fqn = setup(spark, "ice_db5", "t1")
    spark.sql(s"ALTER TABLE $fqn CREATE BRANCH audit_branch")
    spark.sql(s"INSERT INTO $fqn.`branch_audit_branch` VALUES (99,'Branched',9.9)")
    println(s"main count before fast-forward: ${count(spark, fqn)}")

    // positional args
    spark.sql(s"CALL cat.system.fast_forward('$fqn', 'main', 'audit_branch')")
    println(s"[positional] main count after fast-forward: ${count(spark, fqn)}")

    spark.sql(s"ALTER TABLE $fqn CREATE OR REPLACE BRANCH audit_branch2")
    spark.sql(s"INSERT INTO $fqn.`branch_audit_branch2` VALUES (100,'Branched2',10.0)")

    // named args
    spark.sql(
      s"""CALL cat.system.fast_forward(
         |  table  => '$fqn',
         |  branch => 'main',
         |  to     => 'audit_branch2'
         |)""".stripMargin)
    println(s"[named]      main count after fast-forward: ${count(spark, fqn)}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 6. expire_snapshots
  // ══════════════════════════════════════════════════════════════════════════

  def testExpireSnapshots(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("expire_snapshots")
    val fqn = setup(spark, "ice_db6", "t1")

    def snapCount = spark.sql(s"SELECT COUNT(*) FROM $fqn.snapshots").collect()(0).getLong(0)
    println(s"snapshot count before: $snapCount")

    // positional — older_than = now()
   // spark.sql(s"CALL cat.system.expire_snapshots('$fqn', now())")
    println(s"[positional older_than=now]   snapshot count: $snapCount")

    spark.sql(s"INSERT INTO $fqn VALUES (5,'Eve',5.0)")
    spark.sql(s"INSERT INTO $fqn VALUES (6,'Frank',6.0)")

    // named — retain_last
//    spark.sql(
//      s"""CALL cat.system.expire_snapshots(
//         |  table       => '$fqn',
//         |  older_than  => now(),
//         |  retain_last => 1
//         |)""".stripMargin)
//    println(s"[named retain_last=1]         snapshot count: $snapCount")

    spark.sql(s"INSERT INTO $fqn VALUES (7,'Grace',7.0)")
    spark.sql(s"INSERT INTO $fqn VALUES (8,'Hank',8.0)")
    val ids = snapshotIds(spark, fqn)

    // named — specific snapshot_ids
    val res= spark.sql(
      s"""CALL cat.system.expire_snapshots(
         |  table        => '$fqn',
         |  snapshot_ids => ARRAY(${ids.head})
         |)""".stripMargin)
    println(s"[named snapshot_ids]          snapshot count: $snapCount")

    res.show()
    spark.sql(s"INSERT INTO $fqn VALUES (9,'Ivy',9.0)")

    // named — stream_results = true
//    val result = spark.sql(
//      s"""CALL cat.system.expire_snapshots(
//         |  table          => '$fqn',
//         |  older_than     => now(),
//         |  retain_last    => 1,
//         |  stream_results => true
//         |)""".stripMargin)
//    println(s"[named stream_results=true]   expired file count: ${result.count()}")
   // result.show(5, truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 7. remove_orphan_files
  // ══════════════════════════════════════════════════════════════════════════

  def testRemoveOrphanFiles(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("remove_orphan_files")
    val fqn = setup(spark, "ice_db7", "t1")

    // dry run
    val dry = spark.sql(s"CALL cat.system.remove_orphan_files(table => '$fqn', dry_run => true)")
    println(s"[dry_run]          orphan candidates: ${dry.count()}")
    dry.show(5, truncate = false)

    // positional — table only
    spark.sql(s"CALL cat.system.remove_orphan_files('$fqn')").show(5, truncate = false)

    // named — explicit older_than
    spark.sql(
      s"""CALL cat.system.remove_orphan_files(
         |  table      => '$fqn',
         |  older_than => TIMESTAMP '2020-01-01 00:00:00'
         |)""".stripMargin).show(5, truncate = false)

    // named — location scoped
    val loc = spark.sql(s"DESCRIBE EXTENDED $fqn")
      .filter("col_name = 'Location'").collect()(0).getString(1)
    spark.sql(
      s"""CALL cat.system.remove_orphan_files(
         |  table    => '$fqn',
         |  location => '$loc/data'
         |)""".stripMargin).show(5, truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 8. rewrite_data_files
  // ══════════════════════════════════════════════════════════════════════════

  def testRewriteDataFiles(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("rewrite_data_files")
    val fqn = setup(spark, "ice_db8", "t1", partitioned = false)
    (5 to 12).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // positional — binpack default
    spark.sql(s"CALL cat.system.rewrite_data_files('$fqn')").show(truncate = false)

    (13 to 18).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // named — explicit binpack with options
    spark.sql(
      s"""CALL cat.system.rewrite_data_files(
         |  table    => '$fqn',
         |  strategy => 'binpack',
         |  options  => map('min-input-files','2','target-file-size-bytes','134217728')
         |)""".stripMargin).show(truncate = false)

    (19 to 24).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // named — sort single column
    spark.sql(
      s"""CALL cat.system.rewrite_data_files(
         |  table      => '$fqn',
         |  strategy   => 'sort',
         |  sort_order => 'id ASC NULLS LAST'
         |)""".stripMargin).show(truncate = false)

    (25 to 30).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // named — z-order
    spark.sql(
      s"""CALL cat.system.rewrite_data_files(
         |  table      => '$fqn',
         |  strategy   => 'sort',
         |  sort_order => 'zorder(id, val)'
         |)""".stripMargin).show(truncate = false)

    (31 to 36).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // named — where filter
    spark.sql(
      s"""CALL cat.system.rewrite_data_files(
         |  table    => '$fqn',
         |  strategy => 'binpack',
         |  where    => 'id < 10'
         |)""".stripMargin).show(truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 9. rewrite_manifests
  // ══════════════════════════════════════════════════════════════════════════

  def testRewriteManifests(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("rewrite_manifests")
    val fqn = setup(spark, "ice_db9", "t1")
    (5 to 15).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // positional
    spark.sql(s"CALL cat.system.rewrite_manifests('$fqn')").show(truncate = false)

    (16 to 25).foreach(i => spark.sql(s"INSERT INTO $fqn VALUES ($i,'User$i',${i.toDouble})"))

    // named — use_caching = false
    spark.sql(
      s"""CALL cat.system.rewrite_manifests(
         |  table       => '$fqn',
         |  use_caching => false
         |)""".stripMargin).show(truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 10. snapshot (non-destructive copy parquet → iceberg)
  // ══════════════════════════════════════════════════════════════════════════

  def testSnapshot(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("snapshot (migration)")
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ice_db10")
    spark.sql("DROP TABLE IF EXISTS cat.ice_db10.src")
    spark.sql("CREATE TABLE cat.ice_db10.src (id INT, name STRING) USING parquet")
    spark.sql("INSERT INTO cat.ice_db10.src VALUES (1,'A'),(2,'B')")

    spark.sql("DROP TABLE IF EXISTS cat.ice_db10.ice_snap1")

    // positional — source, dest
    spark.sql("CALL cat.system.snapshot('cat.ice_db10.src', 'cat.ice_db10.ice_snap1')")
    println(s"[positional] count: ${count(spark, "cat.ice_db10.ice_snap1")}")

    spark.sql("DROP TABLE IF EXISTS cat.ice_db10.ice_snap2")

    // named — with explicit location
    spark.sql(
      s"""CALL cat.system.snapshot(
         |  source_table => 'cat.ice_db10.src',
         |  table        => 'cat.ice_db10.ice_snap2',
         |  location     => '/tmp/ice_snap2'
         |)""".stripMargin)
    println(s"[named location] count: ${count(spark, "cat.ice_db10.ice_snap2")}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 11. migrate (in-place parquet → iceberg)
  // ══════════════════════════════════════════════════════════════════════════

  def testMigrate(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("migrate")
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ice_db11")

    spark.sql("DROP TABLE IF EXISTS cat.ice_db11.parq1")
    spark.sql("CREATE TABLE cat.ice_db11.parq1 (id INT, name STRING) USING parquet")
    spark.sql("INSERT INTO cat.ice_db11.parq1 VALUES (1,'A'),(2,'B')")

    // positional
    spark.sql("CALL cat.system.migrate('cat.ice_db11.parq1')")
    println(s"[positional] provider: " +
      spark.sql("DESCRIBE EXTENDED cat.ice_db11.parq1")
        .filter("col_name = 'Provider'").collect()(0).getString(1))

    spark.sql("DROP TABLE IF EXISTS cat.ice_db11.parq2")
    spark.sql("CREATE TABLE cat.ice_db11.parq2 (id INT, name STRING) USING parquet")
    spark.sql("INSERT INTO cat.ice_db11.parq2 VALUES (1,'A'),(2,'B')")

    // named — with table properties
    spark.sql(
      s"""CALL cat.system.migrate(
         |  table      => 'cat.ice_db11.parq2',
         |  properties => map('write.format.default','parquet','format-version','2')
         |)""".stripMargin)
    println(s"[named properties] provider: " +
      spark.sql("DESCRIBE EXTENDED cat.ice_db11.parq2")
        .filter("col_name = 'Provider'").collect()(0).getString(1))
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 12. add_files
  // ══════════════════════════════════════════════════════════════════════════

  def testAddFiles(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("add_files")
    spark.sql("CREATE DATABASE IF NOT EXISTS cat.ice_db12")
    spark.sql("DROP TABLE IF EXISTS cat.ice_db12.src_parq")
    spark.sql(
      """CREATE TABLE cat.ice_db12.src_parq (id INT, name STRING, part STRING)
        |USING parquet PARTITIONED BY (part)""".stripMargin)
    spark.sql("INSERT INTO cat.ice_db12.src_parq VALUES (1,'A','p1'),(2,'B','p2')")

    spark.sql("DROP TABLE IF EXISTS cat.ice_db12.ice_dest")
    spark.sql(
      """CREATE TABLE cat.ice_db12.ice_dest (id INT, name STRING, part STRING)
        |USING iceberg PARTITIONED BY (part)""".stripMargin)

    // named — all partitions
    spark.sql(
      s"""CALL cat.system.add_files(
         |  table        => 'cat.ice_db12.ice_dest',
         |  source_table => 'cat.ice_db12.src_parq'
         |)""".stripMargin)
    println(s"[named all partitions]  count: ${count(spark, "cat.ice_db12.ice_dest")}")

    spark.sql("TRUNCATE TABLE cat.ice_db12.ice_dest")

    // named — partition_filter
    spark.sql(
      s"""CALL cat.system.add_files(
         |  table            => 'cat.ice_db12.ice_dest',
         |  source_table     => 'cat.ice_db12.src_parq',
         |  partition_filter => map('part','p1')
         |)""".stripMargin)
    println(s"[named partition_filter=p1] count: ${count(spark, "cat.ice_db12.ice_dest")}")
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 13. ancestors_of
  // ══════════════════════════════════════════════════════════════════════════

  def testAncestorsOf(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("ancestors_of")
    val fqn = setup(spark, "ice_db13", "t1")
    val ids  = snapshotIds(spark, fqn)

    // positional — current snapshot
    println("[positional current]")
    spark.sql(s"CALL cat.system.ancestors_of('$fqn')").show(truncate = false)

    // positional — specific snapshot_id
    println(s"[positional snapshot_id=${ids.last}]")
    spark.sql(s"CALL cat.system.ancestors_of('$fqn', ${ids.last})").show(truncate = false)

    // named — current
    println("[named current]")
    spark.sql(s"CALL cat.system.ancestors_of(table => '$fqn')").show(truncate = false)

    // named — specific snapshot_id
    println(s"[named snapshot_id=${ids.head}]")
    spark.sql(s"CALL cat.system.ancestors_of(table => '$fqn', snapshot_id => ${ids.head})")
      .show(truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 14. create_changelog_view
  // ══════════════════════════════════════════════════════════════════════════

  def testCreateChangelogView(spark: org.apache.spark.sql.SparkSession): Unit = {
    sep("create_changelog_view")
    val fqn = setup(spark, "ice_db14", "t1", partitioned = false)
    spark.sql(s"UPDATE $fqn SET val = 99.0 WHERE id = 1")
    spark.sql(s"DELETE FROM $fqn WHERE id = 2")
    val ids = snapshotIds(spark, fqn)

    // minimal named
    spark.sql(
      s"""CALL cat.system.create_changelog_view(
         |  table          => '$fqn',
         |  changelog_view => 'changelog_v1'
         |)""".stripMargin)
    println("[minimal named]")
    spark.sql("SELECT * FROM changelog_v1").show(truncate = false)

    // named — snapshot range
    spark.sql(
      s"""CALL cat.system.create_changelog_view(
         |  table             => '$fqn',
         |  start_snapshot_id => ${ids.head},
         |  end_snapshot_id   => ${ids.last},
         |  changelog_view    => 'changelog_v2'
         |)""".stripMargin)
    println("[named snapshot range]")
    spark.sql("SELECT * FROM changelog_v2").show(truncate = false)

    // full options
    spark.sql(
      s"""CALL cat.system.create_changelog_view(
         |  table              => '$fqn',
         |  start_snapshot_id  => ${ids.head},
         |  end_snapshot_id    => ${ids.last},
         |  changelog_view     => 'changelog_v3',
         |  identifier_columns => ARRAY('id'),
         |  compute_updates    => true,
         |  remove_carryovers  => true
         |)""".stripMargin)
    println("[full options: compute_updates + remove_carryovers]")
    spark.sql("SELECT * FROM changelog_v3").show(truncate = false)
  }

  // ══════════════════════════════════════════════════════════════════════════
  // run all
  // ══════════════════════════════════════════════════════════════════════════

  def runAll(spark: org.apache.spark.sql.SparkSession): Unit = {
  //  testRollbackToSnapshot(spark)
   // testRollbackToTimestamp(spark)
  //  testSetCurrentSnapshot(spark)
  //  testCherrypickSnapshot(spark)
  //  testFastForward(spark)
    testExpireSnapshots(spark)
//    testRemoveOrphanFiles(spark)
//    testRewriteDataFiles(spark)
//    testRewriteManifests(spark)
//    testSnapshot(spark)
//    testMigrate(spark)
//    testAddFiles(spark)
//    testAncestorsOf(spark)
//    testCreateChangelogView(spark)
    println("\n✓ all Iceberg procedure tests done")
  }
}