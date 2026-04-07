package org.apache.spark.sql.hive.plan.spark.sql.execution.plan

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{SupportsNamespaces, TableSchemaChangeCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import scala.collection.JavaConverters._

import java.net.URI
case class CreateCatalogTable(catalogName: String, table: CatalogTable, ignoreIfExists: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    if (sessionState.catalog.tableExists(table.identifier)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw QueryCompilationErrors.tableAlreadyExistsError(table.identifier.unquotedString)
      }
    }else{
      val tableURI = getTablePathTable(sparkSession)
      val newTable = table.copy(storage = table.storage.copy(locationUri = tableURI))
      val dbPath = sparkSession.sessionState.catalog.getDatabaseMetadata(table.database).locationUri
      SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[SupportsNamespaces].createNamespace(Array(table.database), Map("location"->dbPath.toString).asJava)
      SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableSchemaChangeCatalog].registerTableInMetastore(newTable, ignoreIfExists)

      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
      Seq.empty[Row]

    }
  }


  def getTablePathTable(sparkSession: SparkSession):Option[URI]={

    val dbPath = sparkSession.sessionState.catalog.getDatabaseMetadata(table.database).locationUri
    val dbStringPath = if (dbPath.toString.endsWith("/")) {
      dbPath.toString
    } else {
      dbPath.toString + "/"
    }
    var location = table.storage.locationUri
    location = location match {
      case None =>
        Some(CatalogUtils.stringToURI(dbStringPath + table.identifier.table))
      case Some(v) => Some(v)

    }
    location
  }
}
