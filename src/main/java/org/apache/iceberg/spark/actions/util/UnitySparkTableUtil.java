package org.apache.iceberg.spark.actions.util;


import static org.apache.iceberg.spark.SparkSchemaUtil.schemaForTable;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.*;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkExceptionUtil;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.hive.catalog.UnityCatalogUtil;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Function2;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;
import scala.runtime.AbstractPartialFunction;

/**
 * Java version of the original SparkTableUtil.scala
 * https://github.com/apache/iceberg/blob/apache-iceberg-0.8.0-incubating/spark/src/main/scala/org/apache/iceberg/spark/SparkTableUtil.scala
 */
public class UnitySparkTableUtil {

    private static final String DUPLICATE_FILE_MESSAGE =
            "Cannot complete import because data files "
                    + "to be imported already exist within the target table: %s.  "
                    + "This is disabled by default as Iceberg is not designed for multiple references to the same file"
                    + " within the same table.  If you are sure, you may set 'check_duplicate_files' to false to force the import.";



    /**
     * Returns a DataFrame with a row for each partition in the table.
     *
     * <p>The DataFrame has 3 columns, partition key (a=1/b=2), partition location, and format (avro
     * or parquet).
     *
     * @param spark a Spark session
     * @param table a table name and (optional) database
     * @return a DataFrame of the table's partitions
     */
    public static Dataset<Row> partitionDF(SparkSession spark, String table) {
        List<SparkPartition> partitions = getPartitions(spark, table);
        return spark
                .createDataFrame(partitions, org.apache.iceberg.spark.SparkTableUtil.SparkPartition.class)
                .toDF("partition", "uri", "format");
    }

    /**
     * Returns a DataFrame with a row for each partition that matches the specified 'expression'.
     *
     * @param spark a Spark session.
     * @param table name of the table.
     * @param expression The expression whose matching partitions are returned.
     * @return a DataFrame of the table partitions.
     */
    public static Dataset<Row> partitionDFByFilter(
            SparkSession spark, String table, String expression) {
        List<SparkPartition> partitions = getPartitionsByFilter(spark, table, expression);
        return spark
                .createDataFrame(partitions, org.apache.iceberg.spark.SparkTableUtil.SparkPartition.class)
                .toDF("partition", "uri", "format");
    }

    /**
     * Returns all partitions in the table.
     *
     * @param spark a Spark session
     * @param table a table name and (optional) database
     * @return all table's partitions
     */
    public static List<SparkPartition> getPartitions(SparkSession spark, String table) {
        try {
            TableIdentifier tableIdent = spark.sessionState().sqlParser().parseTableIdentifier(table);
            return getPartitions(spark, tableIdent, null);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e) ;
        }
    }

    /**
     * Returns all partitions in the table.
     *
     * @param spark a Spark session
     * @param tableIdent a table identifier
     * @param partitionFilter partition filter, or null if no filter
     * @return all table's partitions
     */
    public static List<SparkPartition> getPartitions(
            SparkSession spark, TableIdentifier tableIdent, Map<String, String> partitionFilter) {
        try {

            Seq<CatalogTablePartition> partitions =
                    (new UnityCatalogUtil(spark)).getCatalogTablePartitions(tableIdent).toIndexedSeq();
            CatalogTable catalogTable = (new UnityCatalogUtil(spark)).getV1CatalogTableFromV2Table(tableIdent) ;
            if(partitionFilter!=null){
                return JavaConverters.seqAsJavaListConverter(partitions).asJava().stream()
                        .map(catalogPartition -> toSparkPartition(catalogPartition, catalogTable)).filter(p -> p.getValues().entrySet().containsAll(partitionFilter.entrySet()))
                        .collect(Collectors.toList());
            }else {
                return JavaConverters.seqAsJavaListConverter(partitions).asJava().stream()
                        .map(catalogPartition -> toSparkPartition(catalogPartition, catalogTable))
                        .collect(Collectors.toList());
            }
        }  catch (Exception e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unknown table: %s. Table not found in catalog.", tableIdent);
        }
    }

    /**
     * Returns partitions that match the specified 'predicate'.
     *
     * @param spark a Spark session
     * @param table a table name and (optional) database
     * @param predicate a predicate on partition columns
     * @return matching table's partitions
     */
    public static List<SparkPartition> getPartitionsByFilter(
            SparkSession spark, String table, String predicate) {
        TableIdentifier tableIdent;
        try {
            tableIdent = spark.sessionState().sqlParser().parseTableIdentifier(table);
        } catch (ParseException e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unable to parse the table identifier: %s", table);
        }

        Expression unresolvedPredicateExpr;
        try {
            unresolvedPredicateExpr = spark.sessionState().sqlParser().parseExpression(predicate);
        } catch (ParseException e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unable to parse the predicate expression: %s", predicate);
        }

        Expression resolvedPredicateExpr = resolveAttrs(spark, table, unresolvedPredicateExpr);
        return getPartitionsByFilter(spark, tableIdent, resolvedPredicateExpr);
    }

    /**
     * Returns partitions that match the specified 'predicate'.
     *
     * @param spark a Spark session
     * @param tableIdent a table identifier
     * @param predicateExpr a predicate expression on partition columns
     * @return matching table's partitions
     */
    public static List<SparkPartition> getPartitionsByFilter(
            SparkSession spark, TableIdentifier tableIdent, Expression predicateExpr) {
        try {
            SessionCatalog catalog = spark.sessionState().catalog();
            CatalogTable catalogTable = catalog.getTableMetadata(tableIdent);

            Expression resolvedPredicateExpr;
            if (!predicateExpr.resolved()) {
                resolvedPredicateExpr = resolveAttrs(spark, tableIdent.quotedString(), predicateExpr);
            } else {
                resolvedPredicateExpr = predicateExpr;
            }
            Seq<Expression> predicates =
                    JavaConverters.collectionAsScalaIterableConverter(ImmutableList.of(resolvedPredicateExpr))
                            .asScala()
                            .toIndexedSeq();

            Seq<CatalogTablePartition> partitions =
                    catalog.listPartitionsByFilter(tableIdent, predicates).toIndexedSeq();

            return JavaConverters.seqAsJavaListConverter(partitions).asJava().stream()
                    .map(catalogPartition -> toSparkPartition(catalogPartition, catalogTable))
                    .collect(Collectors.toList());
        } catch (NoSuchDatabaseException e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unknown table: %s. Database not found in catalog.", tableIdent);
        } catch (NoSuchTableException e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unknown table: %s. Table not found in catalog.", tableIdent);
        }
    }

    private static List<DataFile> listPartition(
            SparkPartition partition,
            PartitionSpec spec,
            SerializableConfiguration conf,
            MetricsConfig metricsConfig,
            NameMapping mapping) {
        return TableMigrationUtil.listPartition(
                partition.values,
                partition.uri,
                partition.format,
                spec,
                conf.get(),
                metricsConfig,
                mapping);
    }

    private static String getTableProvider(CatalogTable table){
        String format = table.provider().get();
        if(format.equalsIgnoreCase("hive")){
            format = table.storage().properties().get("fileformat").get() ;
        }
        return format ;
    }

    private static SparkPartition toSparkPartition(
            CatalogTablePartition partition, CatalogTable table) {
        Option<URI> locationUri = partition.storage().locationUri();
        Option<String> serde = partition.storage().serde();


        String uri = Util.uriToString(locationUri.get());
        String format = getTableProvider(table) ;

        Map<String, String> partitionSpec =
                JavaConverters.mapAsJavaMapConverter(partition.spec()).asJava();
        return new SparkPartition(partitionSpec, uri, format);
    }

    private static Expression resolveAttrs(SparkSession spark, String table, Expression expr) {
        Function2<String, String, Object> resolver = spark.sessionState().analyzer().resolver();
        LogicalPlan plan = spark.table(table).queryExecution().analyzed();
        return expr.transform(
                new AbstractPartialFunction<Expression, Expression>() {
                    @Override
                    public Expression apply(Expression attr) {
                        UnresolvedAttribute unresolvedAttribute = (UnresolvedAttribute) attr;
                        Option<NamedExpression> namedExpressionOption =
                                plan.resolve(unresolvedAttribute.nameParts(), resolver);
                        if (namedExpressionOption.isDefined()) {
                            return (Expression) namedExpressionOption.get();
                        } else {
                            throw new IllegalArgumentException(
                                    String.format("Could not resolve %s using columns: %s", attr, plan.output()));
                        }
                    }

                    @Override
                    public boolean isDefinedAt(Expression attr) {
                        return attr instanceof UnresolvedAttribute;
                    }
                });
    }

    private static Iterator<ManifestFile> buildManifest(
            SerializableConfiguration conf,
            PartitionSpec spec,
            String basePath,
            Iterator<Tuple2<String, DataFile>> fileTuples) {
        if (fileTuples.hasNext()) {
            FileIO io = new HadoopFileIO(conf.get());
            TaskContext ctx = TaskContext.get();
            String suffix =
                    String.format(
                            "stage-%d-task-%d-manifest-%s",
                            ctx.stageId(), ctx.taskAttemptId(), UUID.randomUUID());
            Path location = new Path(basePath, suffix);
            String outputPath = FileFormat.AVRO.addExtension(location.toString());
            OutputFile outputFile = io.newOutputFile(outputPath);
            ManifestWriter<DataFile> writer = ManifestFiles.write(spec, outputFile);

            try (ManifestWriter<DataFile> writerRef = writer) {
                fileTuples.forEachRemaining(fileTuple -> writerRef.add(fileTuple._2));
            } catch (IOException e) {
                throw SparkExceptionUtil.toUncheckedException(
                        e, "Unable to close the manifest writer: %s", outputPath);
            }

            ManifestFile manifestFile = writer.toManifestFile();
            return ImmutableList.of(manifestFile).iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * Import files from an existing Spark table to an Iceberg table.
     *
     * <p>The import uses the Spark session to get table metadata. It assumes no operation is going on
     * the original and target table and thus is not thread-safe.
     *
     * @param spark a Spark session
     * @param sourceTableIdent an identifier of the source Spark table
     * @param targetTable an Iceberg table where to import the data
     * @param stagingDir a staging directory to store temporary manifest files
     * @param partitionFilter only import partitions whose values match those in the map, can be
     *     partially defined
     * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
     */
    public static void importSparkTable(
            String catalogName,
            SparkSession spark,
            TableIdentifier sourceTableIdent,
            Table targetTable,
            String stagingDir,
            Map<String, String> partitionFilter,
            boolean checkDuplicateFiles) {
        TableCatalog catalog = (TableCatalog)spark.sessionState().catalogManager().catalog(catalogName) ;

        String db =
                sourceTableIdent.database().nonEmpty()
                        ? sourceTableIdent.database().get()
                        : "default";
        TableIdentifier sourceTableIdentWithDB =
                new TableIdentifier(sourceTableIdent.table(), Some.apply(db), Some.apply(catalogName));

        Identifier sourceCatalogTable = Identifier.of(Namespace.of(db).levels(), sourceTableIdent.table()) ;
        if (!catalog.tableExists(sourceCatalogTable)) {
            throw new org.apache.iceberg.exceptions.NoSuchTableException(
                    "Table %s does not exist", sourceTableIdentWithDB);
        }

        try {
            PartitionSpec spec =
                    specForTable(spark, sourceTableIdentWithDB.unquotedString(),catalogName);

            if (Objects.equal(spec, PartitionSpec.unpartitioned())) {
                importUnpartitionedSparkTable(
                        spark, sourceTableIdentWithDB, targetTable, checkDuplicateFiles);
            } else {
                List<SparkPartition> sourceTablePartitions =
                        getPartitions(spark, sourceTableIdentWithDB, partitionFilter);
                if (sourceTablePartitions.isEmpty()) {
                    targetTable.newAppend().commit();
                } else {
                    importSparkPartitions(
                            spark, sourceTablePartitions, targetTable, spec, stagingDir, checkDuplicateFiles);
                }
            }
        } catch (AnalysisException e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unable to get partition spec for table: %s", sourceTableIdentWithDB);
        }
    }

    public static PartitionSpec specForTable(SparkSession spark, String name, String catalogName) throws AnalysisException {
        List<String> parts = Lists.newArrayList(Splitter.on('.').limit(3).split(name));
        String db = parts.size() == 1 ? "default" : (String)parts.get(1);
        String table = (String)parts.get(parts.size() == 1 ? 0 : 2);
        PartitionSpec spec = identitySpec(schemaForTable(spark, name), (Collection)((new UnityCatalogUtil(spark)).listColumns(catalogName,db, table).collectAsList()));
        return spec == null ? PartitionSpec.unpartitioned() : spec;
    }

    private static PartitionSpec identitySpec(Schema schema, Collection<org.apache.spark.sql.catalog.Column> columns) {
        List<String> names = Lists.newArrayList();
        for (org.apache.spark.sql.catalog.Column column : columns) {
            if (column.isPartition()) {
                names.add(column.name());
            }
        }

        return identitySpec(schema, names);
    }

    private static PartitionSpec identitySpec(Schema schema, List<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return null;
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String partitionName : partitionNames) {
            builder.identity(partitionName);
        }

        return builder.build();
    }

    /**
     * Import files from an existing Spark table to an Iceberg table.
     *
     * <p>The import uses the Spark session to get table metadata. It assumes no operation is going on
     * the original and target table and thus is not thread-safe.
     *
     * @param spark a Spark session
     * @param sourceTableIdent an identifier of the source Spark table
     * @param targetTable an Iceberg table where to import the data
     * @param stagingDir a staging directory to store temporary manifest files
     * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
     */
    public static void importSparkTable(
            String catalogName,
            SparkSession spark,
            TableIdentifier sourceTableIdent,
            Table targetTable,
            String stagingDir,
            boolean checkDuplicateFiles) {
        importSparkTable(
                catalogName,
                spark,
                sourceTableIdent,
                targetTable,
                stagingDir,
                Collections.emptyMap(),
                checkDuplicateFiles);
    }

    /**
     * Import files from an existing Spark table to an Iceberg table.
     *
     * <p>The import uses the Spark session to get table metadata. It assumes no operation is going on
     * the original and target table and thus is not thread-safe.
     *
     * @param spark a Spark session
     * @param sourceTableIdent an identifier of the source Spark table
     * @param targetTable an Iceberg table where to import the data
     * @param stagingDir a staging directory to store temporary manifest files
     */
    public static void importSparkTable(String catalogName,
            SparkSession spark, TableIdentifier sourceTableIdent, Table targetTable, String stagingDir) {
        importSparkTable(
                catalogName,spark, sourceTableIdent, targetTable, stagingDir, Collections.emptyMap(), false);
    }

    private static void importUnpartitionedSparkTable(
            SparkSession spark,
            TableIdentifier sourceTableIdent,
            Table targetTable,
            boolean checkDuplicateFiles) {
        try {
            Tuple2<String, String> location = (new UnityCatalogUtil(spark)).getTableLocation(sourceTableIdent) ;


            Map<String, String> partition = Collections.emptyMap();
            PartitionSpec spec = PartitionSpec.unpartitioned();
            Configuration conf = spark.sessionState().newHadoopConf();
            MetricsConfig metricsConfig = MetricsConfig.forTable(targetTable);
            String nameMappingString = targetTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
            NameMapping nameMapping =
                    nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

            List<DataFile> files =
                    TableMigrationUtil.listPartition(
                            partition,
                            location._2(),
                            location._1(),
                            spec,
                            conf,
                            metricsConfig,
                            nameMapping);

            if (checkDuplicateFiles) {
                Dataset<Row> importedFiles =
                        spark
                                .createDataset(Lists.transform(files, f -> f.path().toString()), Encoders.STRING())
                                .toDF("file_path");
                Dataset<Row> existingFiles =
                        loadMetadataTable(spark, targetTable, MetadataTableType.ENTRIES).filter("status != 2");
                Column joinCond =
                        existingFiles.col("data_file.file_path").equalTo(importedFiles.col("file_path"));
                Dataset<String> duplicates =
                        importedFiles.join(existingFiles, joinCond).select("file_path").as(Encoders.STRING());
                Preconditions.checkState(
                        duplicates.isEmpty(),
                        String.format(
                                DUPLICATE_FILE_MESSAGE, Joiner.on(",").join((String[]) duplicates.take(10))));
            }

            AppendFiles append = targetTable.newAppend();
            files.forEach(append::appendFile);
            append.commit();
        }  catch (Exception e) {
            throw SparkExceptionUtil.toUncheckedException(
                    e, "Unknown table: %s. Table not found in catalog.", sourceTableIdent);
        }
    }

    /**
     * Import files from given partitions to an Iceberg table.
     *
     * @param spark a Spark session
     * @param partitions partitions to import
     * @param targetTable an Iceberg table where to import the data
     * @param spec a partition spec
     * @param stagingDir a staging directory to store temporary manifest files
     * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
     */
    public static void importSparkPartitions(
            SparkSession spark,
            List<SparkPartition> partitions,
            Table targetTable,
            PartitionSpec spec,
            String stagingDir,
            boolean checkDuplicateFiles) {
        Configuration conf = spark.sessionState().newHadoopConf();
        SerializableConfiguration serializableConf = new SerializableConfiguration(conf);
        int parallelism =
                Math.min(
                        partitions.size(), spark.sessionState().conf().parallelPartitionDiscoveryParallelism());
        int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();
        MetricsConfig metricsConfig = MetricsConfig.fromProperties(targetTable.properties());
        String nameMappingString = targetTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        NameMapping nameMapping =
                nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<SparkPartition> partitionRDD = sparkContext.parallelize(partitions, parallelism);

        Dataset<SparkPartition> partitionDS =
                spark.createDataset(partitionRDD.rdd(), Encoders.javaSerialization(SparkPartition.class));

        Dataset<DataFile> filesToImport =
                partitionDS.flatMap(
                        (FlatMapFunction<SparkPartition, DataFile>)
                                sparkPartition ->
                                        listPartition(
                                                sparkPartition, spec, serializableConf, metricsConfig, nameMapping)
                                                .iterator(),
                        Encoders.javaSerialization(DataFile.class));

        if (checkDuplicateFiles) {
            Dataset<Row> importedFiles =
                    filesToImport
                            .map((MapFunction<DataFile, String>) f -> f.path().toString(), Encoders.STRING())
                            .toDF("file_path");
            Dataset<Row> existingFiles =
                    loadMetadataTable(spark, targetTable, MetadataTableType.ENTRIES).filter("status != 2");
            Column joinCond =
                    existingFiles.col("data_file.file_path").equalTo(importedFiles.col("file_path"));
            Dataset<String> duplicates =
                    importedFiles.join(existingFiles, joinCond).select("file_path").as(Encoders.STRING());
            Preconditions.checkState(
                    duplicates.isEmpty(),
                    String.format(
                            DUPLICATE_FILE_MESSAGE, Joiner.on(",").join((String[]) duplicates.take(10))));
        }

        List<ManifestFile> manifests =
                filesToImport
                        .repartition(numShufflePartitions)
                        .map(
                                (MapFunction<DataFile, Tuple2<String, DataFile>>)
                                        file -> Tuple2.apply(file.path().toString(), file),
                                Encoders.tuple(Encoders.STRING(), Encoders.javaSerialization(DataFile.class)))
                        .orderBy(col("_1"))
                        .mapPartitions(
                                (MapPartitionsFunction<Tuple2<String, DataFile>, ManifestFile>)
                                        fileTuple -> buildManifest(serializableConf, spec, stagingDir, fileTuple),
                                Encoders.javaSerialization(ManifestFile.class))
                        .collectAsList();

        try {
            TableOperations ops = ((HasTableOperations) targetTable).operations();
            int formatVersion = ops.current().formatVersion();
            boolean snapshotIdInheritanceEnabled =
                    PropertyUtil.propertyAsBoolean(
                            targetTable.properties(),
                            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
                            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);

            AppendFiles append = targetTable.newAppend();
            manifests.forEach(append::appendManifest);
            append.commit();

            if (formatVersion == 1 && !snapshotIdInheritanceEnabled) {
                // delete original manifests as they were rewritten before the commit
                deleteManifests(targetTable.io(), manifests);
            }
        } catch (Throwable e) {
            deleteManifests(targetTable.io(), manifests);
            throw e;
        }
    }

    /**
     * Import files from given partitions to an Iceberg table.
     *
     * @param spark a Spark session
     * @param partitions partitions to import
     * @param targetTable an Iceberg table where to import the data
     * @param spec a partition spec
     * @param stagingDir a staging directory to store temporary manifest files
     */
    public static void importSparkPartitions(
            SparkSession spark,
            List<SparkPartition> partitions,
            Table targetTable,
            PartitionSpec spec,
            String stagingDir) {
        importSparkPartitions(spark, partitions, targetTable, spec, stagingDir, false);
    }

    public static List<org.apache.iceberg.spark.SparkTableUtil.SparkPartition> filterPartitions(
            List<org.apache.iceberg.spark.SparkTableUtil.SparkPartition> partitions, Map<String, String> partitionFilter) {
        if (partitionFilter.isEmpty()) {
            return partitions;
        } else {
            return partitions.stream()
                    .filter(p -> p.getValues().entrySet().containsAll(partitionFilter.entrySet()))
                    .collect(Collectors.toList());
        }
    }

    private static void deleteManifests(FileIO io, List<ManifestFile> manifests) {
        Tasks.foreach(manifests)
                .executeWith(ThreadPools.getWorkerPool())
                .noRetry()
                .suppressFailureWhenFinished()
                .run(item -> io.deleteFile(item.path()));
    }

    public static Dataset<Row> loadMetadataTable(
            SparkSession spark, Table table, MetadataTableType type) {
        return loadMetadataTable(spark, table, type, ImmutableMap.of());
    }

    public static Dataset<Row> loadMetadataTable(
            SparkSession spark, Table table, MetadataTableType type, Map<String, String> extraOptions) {
        SparkTable metadataTable =
                new SparkTable(MetadataTableUtils.createMetadataTableInstance(table, type), false);
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(extraOptions);
        return Dataset.ofRows(
                spark, DataSourceV2Relation.create(metadataTable, Some.empty(), Some.empty(), options));
    }

    /**
     * Determine the write branch.
     *
     * <p>Validate wap config and determine the write branch.
     *
     * @param spark a Spark Session
     * @param branch write branch if there is no WAP branch configured
     * @return branch for write operation
     */
    public static String determineWriteBranch(SparkSession spark, String branch) {
        String wapId = spark.conf().get(SparkSQLProperties.WAP_ID, null);
        String wapBranch = spark.conf().get(SparkSQLProperties.WAP_BRANCH, null);
        ValidationException.check(
                wapId == null || wapBranch == null,
                "Cannot set both WAP ID and branch, but got ID [%s] and branch [%s]",
                wapId,
                wapBranch);

        if (wapBranch != null) {
            ValidationException.check(
                    branch == null,
                    "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [%s]",
                    branch,
                    wapBranch);

            return wapBranch;
        }
        return branch;
    }

    public static boolean wapEnabled(Table table) {
        return PropertyUtil.propertyAsBoolean(
                table.properties(),
                TableProperties.WRITE_AUDIT_PUBLISH_ENABLED,
                Boolean.getBoolean(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
    }

    /** Class representing a table partition. */
    public static class SparkPartition implements Serializable {
        public Map<String, String> values;
        public String uri;
        public  String format;

        public SparkPartition(Map<String, String> values, String uri, String format) {
            this.values = Maps.newHashMap(values);
            this.uri = uri;
            this.format = format;
        }

        public Map<String, String> getValues() {
            return values;
        }

        public String getUri() {
            return uri;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("values", values)
                    .add("uri", uri)
                    .add("format", format)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SparkPartition that = (SparkPartition) o;
            return Objects.equal(values, that.values)
                    && Objects.equal(uri, that.uri)
                    && Objects.equal(format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values, uri, format);
        }
    }
}

