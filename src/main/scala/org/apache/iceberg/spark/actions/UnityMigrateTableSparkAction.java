package org.apache.iceberg.spark.actions;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.actions.ImmutableMigrateTable;
import org.apache.iceberg.actions.ImmutableMigrateTable.Result;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.actions.util.UnitySparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;


import java.util.Map;

public class UnityMigrateTableSparkAction extends UnityBaseTableCreationSparkAction<UnityMigrateTableSparkAction> implements MigrateTable {

    private static final Logger LOG = LoggerFactory.getLogger(UnityMigrateTableSparkAction.class);
    private static final String BACKUP_SUFFIX = "_BACKUP_";
    private StagingTableCatalog destCatalog;
    private Identifier destTableIdent;
    private Identifier backupIdent;
    private boolean dropBackup = false;


    public UnityMigrateTableSparkAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
        super(spark,sourceCatalog,sourceTableIdent);
        this.destCatalog = this.checkDestinationCatalog(sourceCatalog);
        this.destTableIdent = sourceTableIdent;
        String backupName = sourceTableIdent.name() + "_BACKUP_";
        this.backupIdent = Identifier.of(sourceTableIdent.namespace(), backupName);
    }

    @Override
    protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
        return (TableCatalog)catalog;
    }

    @Override
    protected StagingTableCatalog destCatalog() {
        return this.destCatalog;
    }

    @Override
    protected Identifier destTableIdent() {
        return this.destTableIdent;
    }

    @Override
    protected Map<String, String> destTableProps() {
        Map<String, String> properties = Maps.newHashMap();
        properties.putAll((Map) JavaConverters.mapAsJavaMapConverter(this.v1SourceTable().properties()).asJava());
        EXCLUDED_PROPERTIES.forEach(properties::remove);
        properties.put("provider", "iceberg");
        properties.putAll(this.additionalProperties());
        properties.put("migrated", "true");
        properties.putIfAbsent("location", this.sourceTableLocation());
        return properties;
    }

    @Override
    public UnityMigrateTableSparkAction tableProperties(Map<String, String> map) {
        this.setProperties(map);
        return this;
    }

    @Override
    public UnityMigrateTableSparkAction tableProperty(String property, String value) {
        this.setProperty(property, value);
        return this;
    }

    @Override
    public Result execute() {
        String desc = String.format("Migrating table %s", this.destTableIdent().toString());
        JobGroupInfo info = this.newJobGroupInfo("MIGRATE-TABLE", desc);
        return (MigrateTable.Result)this.withJobGroupInfo(info, this::doExecute);
    }

    private void dropBackupTable() {
        try {
            this.destCatalog().dropTable(this.backupIdent);
        } catch (Exception var2) {
            LOG.error("Cannot drop the backup table {}, after the migration is completed.", this.backupIdent, var2);
        }

    }

    public UnityMigrateTableSparkAction dropBackup() {
        this.dropBackup = true;
        return this;
    }

    public UnityMigrateTableSparkAction backupTableName(String tableName) {
        this.backupIdent = Identifier.of(this.sourceTableIdent().namespace(), tableName);
        return this;
    }

    private void renameAndBackupSourceTable() {
        try {
            LOG.info("Renaming {} as {} for backup", this.sourceTableIdent(), this.backupIdent);
            this.destCatalog().renameTable(this.sourceTableIdent(), this.backupIdent);
        } catch (NoSuchTableException var2) {
            throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot find source table %s", new Object[]{this.sourceTableIdent()});
        } catch (TableAlreadyExistsException var3) {
            throw new AlreadyExistsException("Cannot rename %s as %s for backup. The backup table already exists.", new Object[]{this.sourceTableIdent(), this.backupIdent});
        }
    }

    private void restoreSourceTable() {
        try {
            LOG.info("Restoring {} from {}", this.sourceTableIdent(), this.backupIdent);
            this.destCatalog().renameTable(this.backupIdent, this.sourceTableIdent());
        } catch (NoSuchTableException var2) {
            LOG.error("Cannot restore the original table, the backup table {} cannot be found", this.backupIdent, var2);
        } catch (TableAlreadyExistsException var3) {
            LOG.error("Cannot restore the original table, a table with the original name exists. Use the backup table {} to restore the original table manually.", this.backupIdent, var3);
        }

    }


    private MigrateTable.Result doExecute() {
        LOG.info("Starting the migration of {} to Iceberg", sourceTableIdent());

        renameAndBackupSourceTable();

        StagedSparkTable stagedTable = null;
        Table icebergTable;
        boolean threw = true;
        try {
            LOG.info("Staging a new Iceberg table {}", destTableIdent());
            stagedTable = stageDestTable();
            icebergTable = stagedTable.table();

            LOG.info("Ensuring {} has a valid name mapping", destTableIdent());
            ensureNameMappingPresent(icebergTable);

            Some<String> backupNamespace = Some.apply(backupIdent.namespace()[0]);
            TableIdentifier v1BackupIdent = new TableIdentifier(backupIdent.name(), backupNamespace);
            String stagingLocation = getMetadataLocation(icebergTable);
            LOG.info("Generating Iceberg metadata for {} in {}", destTableIdent(), stagingLocation);
            UnitySparkTableUtil.importSparkTable(this.destCatalog.name(),spark(), v1BackupIdent, icebergTable, stagingLocation);
            LOG.info("Committing staged changes to {}", destTableIdent());
            stagedTable.commitStagedChanges();
            threw = false;
        } finally {
            if (threw) {
                LOG.error("Failed to perform the migration, aborting table creation and restoring the original table");
                this.restoreSourceTable();
                if (stagedTable != null) {
                    try {
                        stagedTable.abortStagedChanges();
                    } catch (Exception var12) {
                        LOG.error("Cannot abort staged changes", var12);
                    }
                }
            } else if (this.dropBackup) {
                this.dropBackupTable();
            }

        }

        Snapshot snapshot = icebergTable.currentSnapshot();
        long migratedDataFilesCount = Long.parseLong((String)snapshot.summary().get("total-data-files"));
        LOG.info("Successfully loaded Iceberg metadata for {} files to {}", migratedDataFilesCount, this.destTableIdent());
        return ImmutableMigrateTable.Result.builder()
                .migratedDataFilesCount(migratedDataFilesCount)
                .build();
    }

    @Override
    protected UnityMigrateTableSparkAction self() {
        return this;
    }
}
