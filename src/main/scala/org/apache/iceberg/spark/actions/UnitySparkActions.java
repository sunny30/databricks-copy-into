package org.apache.iceberg.spark.actions;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.*;
import org.apache.iceberg.hadoop.UnityHadoopCatalog;
import org.apache.spark.sql.SparkSession;

public class UnitySparkActions implements ActionsProvider {

    private static SparkActions actions ;


    public UnitySparkActions get() {
        this.actions = SparkActions.get(SparkSession.active());
        return new UnitySparkActions(actions) ;
    }

    public UnitySparkActions get(SparkActions actions) {
        this.actions = actions ;
        return new UnitySparkActions(actions) ;
    }
    public UnitySparkActions(){
        actions = SparkActions.get() ;
    }

    public UnitySparkActions(SparkActions actions){
        this.actions = actions;
    }

    public SnapshotTable snapshotTable(String sourceTableIdent) {
        return actions.snapshotTable(sourceTableIdent) ;
    }

    public MigrateTable migrateTable(String tableIdent) {
        return actions.migrateTable(tableIdent) ;
    }

    public DeleteOrphanFiles deleteOrphanFiles(Table table) {
        return actions.deleteOrphanFiles(table) ;
    }

    public RewriteManifests rewriteManifests(Table table) {
        return actions.rewriteManifests(table) ;
    }

    public RewriteDataFiles rewriteDataFiles(Table table) {
        return actions.rewriteDataFiles(table) ;
    }

    public ExpireSnapshots expireSnapshots(Table table) {
        return actions.expireSnapshots(table) ;
    }

    public DeleteReachableFiles deleteReachableFiles(String metadataLocation) {
        return actions.deleteReachableFiles(metadataLocation) ;
    }

    public RewritePositionDeleteFiles rewritePositionDeletes(Table table) {
        return actions.rewritePositionDeletes(table) ;
    }







}
