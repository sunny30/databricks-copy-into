package org.apache.iceberg.spark.actions;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.*;
import org.apache.iceberg.hadoop.UnityHadoopCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

public class UnitySparkActions implements ActionsProvider {

    private static SparkActions actions ;

    private SparkSession spark;


    public static UnitySparkActions get() {
        return new UnitySparkActions(actions) ;
    }

    public static UnitySparkActions get(SparkActions actions) {

        return new UnitySparkActions(actions) ;
    }
    public UnitySparkActions(){
        actions = SparkActions.get() ;
    }

    public UnitySparkActions(SparkActions actions){
        this.spark = SparkSession.active();
        this.actions = SparkActions.get(this.spark);
      //  this.actions = actions;
    }

    public SnapshotTable snapshotTable(String sourceTableIdent) {
        return actions.snapshotTable(sourceTableIdent) ;
    }

    public UnityMigrateTableSparkAction migrateTable(String tableIdent) {
        String ctx = "migrate target";
        CatalogPlugin defaultCatalog = this.spark.sessionState().catalogManager().currentCatalog();
        Spark3Util.CatalogAndIdentifier catalogAndIdent = Spark3Util.catalogAndIdentifier(ctx, this.spark, tableIdent, defaultCatalog);
        return new UnityMigrateTableSparkAction(this.spark, catalogAndIdent.catalog(), catalogAndIdent.identifier());

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
