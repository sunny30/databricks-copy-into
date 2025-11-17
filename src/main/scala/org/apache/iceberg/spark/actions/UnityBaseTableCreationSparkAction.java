package org.apache.iceberg.spark.actions;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table;
import org.apache.spark.sql.types.StructType;

abstract class UnityBaseTableCreationSparkAction<ThisT> extends BaseSparkAction<ThisT> {
    private static final Set<String> ALLOWED_SOURCES = ImmutableSet.of("parquet", "avro", "orc", "hive");
    protected static final String LOCATION = "location";
    protected static final String ICEBERG_METADATA_FOLDER = "metadata";
    protected static final List<String> EXCLUDED_PROPERTIES = ImmutableList.of("path", "transient_lastDdlTime", "serialization.format");
    private V2Table sourceTable;
    private  CatalogTable sourceCatalogTable;
    private  String sourceTableLocation;
    private  TableCatalog sourceCatalog;
    private  Identifier sourceTableIdent;
    private final Map<String, String> additionalProperties = Maps.newHashMap();

    UnityBaseTableCreationSparkAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
        super(spark);
        this.sourceCatalog = this.checkSourceCatalog(sourceCatalog);
        this.sourceTableIdent = sourceTableIdent;

        try {
            this.sourceTable = (V2Table)this.sourceCatalog.loadTable(sourceTableIdent);
            this.sourceCatalogTable = this.sourceTable.v1Table();
        } catch (NoSuchTableException var5) {
            throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot not find source table '%s'", new Object[]{sourceTableIdent});
        } catch (ClassCastException var6) {
            throw new IllegalArgumentException(String.format("Cannot use non-v1 table '%s' as a source", sourceTableIdent), var6);
        }

        this.validateSourceTable();
        this.sourceTableLocation = CatalogUtils.URIToString((URI)this.sourceCatalogTable.storage().locationUri().get());
    }

    protected UnityBaseTableCreationSparkAction(SparkSession spark) {
        super(spark);
    }

    protected abstract TableCatalog checkSourceCatalog(CatalogPlugin var1);

    protected abstract StagingTableCatalog destCatalog();

    protected abstract Identifier destTableIdent();

    protected abstract Map<String, String> destTableProps();

    protected String sourceTableLocation() {
        return this.sourceTableLocation;
    }

    protected CatalogTable v1SourceTable() {
        return this.sourceCatalogTable;
    }

    protected TableCatalog sourceCatalog() {
        return this.sourceCatalog;
    }

    protected Identifier sourceTableIdent() {
        return this.sourceTableIdent;
    }

    protected void setProperties(Map<String, String> properties) {
        this.additionalProperties.putAll(properties);
    }

    protected void setProperty(String key, String value) {
        this.additionalProperties.put(key, value);
    }

    protected Map<String, String> additionalProperties() {
        return this.additionalProperties;
    }

    private void validateSourceTable() {
        String sourceTableProvider = ((String)this.sourceCatalogTable.provider().get()).toLowerCase(Locale.ROOT);
        Preconditions.checkArgument(ALLOWED_SOURCES.contains(sourceTableProvider), "Cannot create an Iceberg table from source provider: '%s'", sourceTableProvider);
        Preconditions.checkArgument(!this.sourceCatalogTable.storage().locationUri().isEmpty(), "Cannot create an Iceberg table from a source without an explicit location");
    }

    protected StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {
        Preconditions.checkArgument(catalog instanceof SparkSessionCatalog || catalog instanceof SparkCatalog, "Cannot create Iceberg table in non-Iceberg Catalog. Catalog '%s' was of class '%s' but '%s' or '%s' are required", catalog.name(), catalog.getClass().getName(), SparkSessionCatalog.class.getName(), SparkCatalog.class.getName());
        return (StagingTableCatalog)catalog;
    }

    protected StagedSparkTable stageDestTable() {
        try {
            Map<String, String> props = this.destTableProps();
            StructType schema = this.sourceTable.schema();
            Transform[] partitioning = this.sourceTable.partitioning();
            return (StagedSparkTable)this.destCatalog().stageCreate(this.destTableIdent(), schema, partitioning, props);
        } catch (NoSuchNamespaceException var4) {
            throw new org.apache.iceberg.exceptions.NoSuchNamespaceException("Cannot create table %s as the namespace does not exist", new Object[]{this.destTableIdent()});
        } catch (TableAlreadyExistsException var5) {
            throw new AlreadyExistsException("Cannot create table %s as it already exists", new Object[]{this.destTableIdent()});
        }
    }

    protected void ensureNameMappingPresent(Table table) {
        if (!table.properties().containsKey("schema.name-mapping.default")) {
            NameMapping nameMapping = MappingUtil.create(table.schema());
            String nameMappingJson = NameMappingParser.toJson(nameMapping);
            table.updateProperties().set("schema.name-mapping.default", nameMappingJson).commit();
        }

    }

    protected String getMetadataLocation(Table table) {
        String defaultValue = LocationUtil.stripTrailingSlash(table.location()) + "/" + "metadata";
        return LocationUtil.stripTrailingSlash((String)table.properties().getOrDefault("write.metadata.path", defaultValue));
    }
}

